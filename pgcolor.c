
#include "postgres.h"
#include "miscadmin.h"
#include "funcapi.h"

#include <unistd.h>

#include "catalog/pg_type.h"
#include "utils/elog.h"
#include "utils/palloc.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/syscache.h"
#include "utils/hashutils.h"
#include "libpq/pqformat.h"
#include "nodes/makefuncs.h"
#include "nodes/extensible.h"
#include "nodes/readfuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/planner.h"
#include "executor/tstoreReceiver.h"
#include "utils/snapmgr.h"
#include "catalog/namespace.h"
#include "executor/executor.h"
#include "utils/builtins.h"
#include "utils/regproc.h"
#include "storage/ipc.h"

#define ExtendedNodeName "PgColorExtendedNode"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

/* a very simple represantation of color with rgb */
typedef struct color
{
	uint8 r;
	uint8 g;
	uint8 b;
} color;


typedef struct PgColorExtendedNode
{
	ExtensibleNode extensible;

	color *interceptedColor;
} PgColorExtendedNode;

typedef struct PgColorScanState
{
	CustomScanState customScanState;  /* underlying custom scan node */
	PlannedStmt *plannedStatement; /* the execution plan */
	PgColorExtendedNode *color;   						/* the color information passed to the execution */
	uint64 queryId;
	bool finishedScan;          /* flag to check if remote scan is finished */
	Tuplestorestate *tuplestorestate; /* tuple store to store distributed results */
} PgColorScanState;

/* Write a Node field */
#define WRITE_NODE_FIELD(fldname) \
	(appendStringInfo(str, " :" CppAsString(fldname) " "), \
	 outNode(str, node->fldname))

#define WRITE_INT_FIELD(fldname) \
	appendStringInfo(str, " :" CppAsString(fldname) " %d", node->fldname)

#define READ_NODE_FIELD(fldname) \
	token = pg_strtok(&length);		/* skip :fldname */ \
	(void) token;				/* in case not used elsewhere */ \
	local_node->fldname = nodeRead(NULL, 0)

#define READ_INT_FIELD(fldname) \
	token = pg_strtok(&length);		/* skip :fldname */ \
	token = pg_strtok(&length);		/* get field value */ \
	local_node->fldname = atoi(token)


#define DatumGetColor(X)	 ((color *) DatumGetPointer(X))
#define ColorGetDatum(X)	 PointerGetDatum(X)
#define PG_GETARG_COLOR(n)	 DatumGetColor(PG_GETARG_DATUM(n))
#define PG_RETURN_COLOR(x)	 return ColorGetDatum(x)

void _PG_init(void);
static color * color_from_str(char *str);
static char *color_to_str(color *c);

Datum color_in(PG_FUNCTION_ARGS);
Datum color_out(PG_FUNCTION_ARGS);
Datum color_recv(PG_FUNCTION_ARGS);
Datum color_send(PG_FUNCTION_ARGS);
Datum rgb_distance(PG_FUNCTION_ARGS);
Datum color_eq(PG_FUNCTION_ARGS);
Datum color_ne(PG_FUNCTION_ARGS);
Datum color_cmp(PG_FUNCTION_ARGS);
Datum color_lt(PG_FUNCTION_ARGS);
Datum color_le(PG_FUNCTION_ARGS);
Datum color_gt(PG_FUNCTION_ARGS);
Datum color_ge(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(color_in);
PG_FUNCTION_INFO_V1(color_out);
PG_FUNCTION_INFO_V1(color_recv);
PG_FUNCTION_INFO_V1(color_send);
PG_FUNCTION_INFO_V1(rgb_distance);
PG_FUNCTION_INFO_V1(color_eq);
PG_FUNCTION_INFO_V1(color_ne);
PG_FUNCTION_INFO_V1(color_cmp);
PG_FUNCTION_INFO_V1(color_lt);
PG_FUNCTION_INFO_V1(color_le);
PG_FUNCTION_INFO_V1(color_gt);
PG_FUNCTION_INFO_V1(color_ge);



static void CopyPgColorExtendedNode(struct ExtensibleNode *target_node, 
									const struct ExtensibleNode *source_node);
static bool EqualPgColorExtendedNode(const struct ExtensibleNode *target_node, 
									 const struct ExtensibleNode *source_node);

static void OutPgColorExtendedNode(struct StringInfoData *str, 
								    const struct ExtensibleNode *raw_node);
static void ReadPgColorExtendedNode(struct ExtensibleNode *node);


const ExtensibleNodeMethods nodeMethods =
{.extnodename = ExtendedNodeName,
 .node_size =  sizeof(PgColorExtendedNode),
 .nodeCopy = CopyPgColorExtendedNode,
 .nodeEqual = EqualPgColorExtendedNode,
 .nodeRead = ReadPgColorExtendedNode,
 .nodeOut = OutPgColorExtendedNode
};

static PlannedStmt * pg_color_planner(Query *parse, 
				      int cursorOptions, 
				      ParamListInfo boundParams);
static Const *FetchColorInFilter(Query *query);
static bool ExtractColorNodes(Node *node, List **colorList);
static Oid TypeOid(Oid schemaId, const char *typeName);

static Node * PgColorCreateScan(CustomScan *scan);
static RangeTblEntry * RemoteScanRangeTableEntry(List *columnNameList);
static PlannedStmt * FinalizePlan(PlannedStmt *localPlan, color *interceptedColor);
static PgColorExtendedNode * GetPgColorExtendedNode(CustomScan *customScan);
static PlannedStmt * GetOriginalPlan(CustomScan *customScan);


CustomScanMethods PgColorCustomScanMethods = {
	"PGColor Scan",
	PgColorCreateScan
};

static void PgColorBeginScan(CustomScanState *node, EState *estate, int eflags);
static TupleTableSlot * PgColorExecScan(CustomScanState *node);
static TupleTableSlot * ReturnTupleFromTuplestore(PgColorScanState *scanState);
static void PgColorEndScan(CustomScanState *node);
static void PgColorReScan(CustomScanState *node);
static EState * ScanStateGetExecutorState(PgColorScanState *scanState);

static CustomExecMethods PgColorCustomExecMethods = {
	.CustomName = "PgColorExecutorScan",
	.BeginCustomScan = PgColorBeginScan,
	.ExecCustomScan = PgColorExecScan,
	.EndCustomScan = PgColorEndScan,
	.ReScanCustomScan = PgColorReScan,
};

static void ColorQueryStatsExecutorsEntry(uint64 queryId, color *color);
static void InitializeColorQueryStats(void);
static void ColorQueryStatsShmemStartup(void);
static void InitializeColorQueryStats(void);
static HTAB * BuildExistingQueryIdHash(void);
static void ColorQueryStatsRemoveExpiredEntries(HTAB *existingQueryIdHash);
static void ColorQueryStatsSynchronizeEntries(void);
static Tuplestorestate * SetupTuplestore(FunctionCallInfo fcinfo, TupleDesc *tupleDescriptor);
static ReturnSetInfo * CheckTuplestoreReturn(FunctionCallInfo fcinfo, TupleDesc *tupdesc);
static int GetPGColorStatsMax(void);
static ReturnSetInfo * FunctionCallGetTupleStore1(PGFunction function, Oid functionId, Datum argument);
static Oid
FunctionOidExtended(const char *schemaName, const char *functionName, int argumentCount,
                                        bool missingOK);

#define fcSetArg(fc, n, argval) \
	(((fc)->args[n].isnull = false), ((fc)->args[n].value = (argval)))

#define COLOR_STATS_DUMP_FILE "pg_stat/color_query_stats.stat"
#define COLOR_STAT_STATEMENTS_COLS 6
#define COLOR_STAT_STATAMENTS_QUERY_ID 0
#define COLOR_STAT_STATAMENTS_USER_ID 1
#define COLOR_STAT_STATAMENTS_DB_ID 2
#define COLOR_STAT_STATAMENTS_COLOR 3
#define COLOR_STAT_STATAMENTS_CALLS 4


#define USAGE_DECREASE_FACTOR (0.99)    /* decreased every ColorQueryStatsEntryDealloc */
#define STICKY_DECREASE_FACTOR (0.50)   /* factor for sticky entries */
#define USAGE_DEALLOC_PERCENT 5         /* free this % of entries at once */
#define USAGE_INIT (1.0)                /* including initial planning */
#define STATS_SHARED_MEM_NAME "color_query_stats"

#define MAX_KEY_LENGTH NAMEDATALEN

/* Magic number identifying the stats file format */
static const uint32 COLOR_QUERY_STATS_FILE_HEADER = 0x0e756e0f;

/* TODO: maximum number of entries in queryStats hash, should controlled by GUC pgcolor.stat_statements_max */
int ColorStatsMax = 50000;

/*
 * Hashtable key that defines the identity of a hashtable entry.  We use the
 * same hash as pg_stat_statements
 */
typedef struct QueryStatsHashKey
{
	Oid userid;                     /* user OID */
	Oid dbid;                       /* database OID */
	uint64 queryid;                 /* query identifier */
	char color[MAX_KEY_LENGTH];
} QueryStatsHashKey;

/*
 * Statistics per query and executor type
 */
typedef struct queryStatsEntry
{
	QueryStatsHashKey key;   /* hash key of entry - MUST BE FIRST */
	int64 calls;       /* # of times executed */
	double usage;      /* hashtable usage factor */
	slock_t mutex;     /* protects the counters only */
} QueryStatsEntry;

/*
 * Global shared state
 */
typedef struct QueryStatsSharedState
{
	LWLockId lock;                      /* protects hashtable search/modification */
	double cur_median_usage;            /* current median usage in hashtable */
} QueryStatsSharedState;

/* lookup table for existing pg_stat_statements entries */
typedef struct ExistingStatsHashKey
{
	Oid userid;                     /* user OID */
	Oid dbid;                       /* database OID */
	uint64 queryid;                 /* query identifier */
} ExistingStatsHashKey;

/* saved hook address in case of unload */
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

/* Links to shared memory state */
static QueryStatsSharedState *queryStats = NULL;
static HTAB *queryStatsHash = NULL;

/*--- Functions --- */

Datum color_query_stats(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(color_query_stats);
PG_FUNCTION_INFO_V1(color_stat_statements_reset);

static Size ColorQueryStatsSharedMemSize(void);

static void ColorQueryStatsShmemStartup(void);
static void ColorQueryStatsShmemShutdown(int code, Datum arg);
static QueryStatsEntry * ColorQueryStatsEntryAlloc(QueryStatsHashKey *key, bool sticky);
static void ColorQueryStatsEntryDealloc(void);
static void ColorQueryStatsEntryReset(void);
static uint32 ColorQuerysStatsHashFn(const void *key, Size keysize);
static int ColorQuerysStatsMatchFn(const void *key1, const void *key2, Size keysize);
static uint32 ExistingStatsHashFn(const void *key, Size keysize);
static int ExistingStatsMatchFn(const void *key1, const void *key2, Size keysize);
static void
CopyPgColorExtendedNode(struct ExtensibleNode *target_node, const struct ExtensibleNode *source_node)
{
	PgColorExtendedNode *targetPlan = (PgColorExtendedNode *) target_node;
	PgColorExtendedNode *sourcePlan = (PgColorExtendedNode *) source_node;

	targetPlan->interceptedColor = palloc0(sizeof(color));

	targetPlan->interceptedColor->r = sourcePlan->interceptedColor->r;
	targetPlan->interceptedColor->g = sourcePlan->interceptedColor->g;
	targetPlan->interceptedColor->b = sourcePlan->interceptedColor->b;
}

static bool
EqualPgColorExtendedNode(const struct ExtensibleNode *target_node, const struct ExtensibleNode *source_node)
{
	PgColorExtendedNode *targetPlan = (PgColorExtendedNode *) target_node;
	PgColorExtendedNode *sourcePlan = (PgColorExtendedNode *) source_node;

	return targetPlan->interceptedColor->r == sourcePlan->interceptedColor->r && 
	       targetPlan->interceptedColor->g == sourcePlan->interceptedColor->g && 
	       targetPlan->interceptedColor->b == sourcePlan->interceptedColor->b;
}

static void
OutPgColorExtendedNode( struct StringInfoData *str, const struct ExtensibleNode *raw_node)
{
	const PgColorExtendedNode *node = (const PgColorExtendedNode *) raw_node;

	WRITE_INT_FIELD(interceptedColor->r);
	WRITE_INT_FIELD(interceptedColor->g);
	WRITE_INT_FIELD(interceptedColor->b);
}


static void
ReadPgColorExtendedNode(struct ExtensibleNode *node)
{
	PgColorExtendedNode *local_node = (PgColorExtendedNode *) node;
	const char		*token;
	int			length;

	READ_INT_FIELD(interceptedColor->r);
	READ_INT_FIELD(interceptedColor->g);
	READ_INT_FIELD(interceptedColor->b);
}

 void
_PG_init(void)
{
	RegisterExtensibleNodeMethods(&nodeMethods);

	planner_hook = pg_color_planner;
	
	InitializeColorQueryStats();
}

static void
InitializeColorQueryStats(void)
{
	RequestAddinShmemSpace(ColorQueryStatsSharedMemSize());

	elog(LOG, "requesting named LWLockTranch for %s", STATS_SHARED_MEM_NAME);
	RequestNamedLWLockTranche(STATS_SHARED_MEM_NAME, 1);

	/* Install hook */
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = ColorQueryStatsShmemStartup;
}

static PlannedStmt *
pg_color_planner(Query *parse, int cursorOptions, ParamListInfo boundParams)
{
	PlannedStmt *standardPlan = NULL;
	color *colorFilter = NULL;
	Const *colorData = NULL;

	standardPlan = standard_planner(parse, cursorOptions, boundParams);
	
	colorData = FetchColorInFilter(parse);

   if (colorData !=  NULL)
   {
	  colorFilter = DatumGetColor(colorData->constvalue);
	  
	  if (log_min_messages <= DEBUG4 ||
          client_min_messages <= DEBUG4)
        {
        	elog(DEBUG4, "Planner intercepted the color %s", color_to_str(colorFilter));
        }


      return FinalizePlan(standardPlan, colorFilter);
   }

   return standardPlan;
}


static Const *
FetchColorInFilter(Query *query)
{
	RangeTblEntry *rangeTableEntry = NULL;
	FromExpr *joinTree = query->jointree;
	Node *quals = NULL;
	List *colorList  = NIL;

    /* we're only interested in SELECT commands */
	if (query->commandType != CMD_SELECT)
	{
		return NULL;
	}


	/* make sure that the only range table in FROM clause */
	if (list_length(query->rtable) != 1)
	{
		return NULL;
	}

	rangeTableEntry = (RangeTblEntry *) linitial(query->rtable);
	if (rangeTableEntry->rtekind != RTE_RELATION)
	{
		return NULL;
	}

	/* WHERE clause should not be empty */
	if (joinTree == NULL || joinTree->quals == NULL)
	{
		return NULL;
	}

	/* convert list of expressions into expression tree for further processing */
	quals = joinTree->quals;
	if (quals != NULL && IsA(quals, List))
	{
		quals = (Node *) make_ands_explicit((List *) quals);
	}

	ExtractColorNodes(quals, &colorList);

	/* we're only interested if there is a single filter */
	if (list_length(colorList) == 1)
	{
		return  linitial(colorList);
	}


	return NULL;
}


static bool
ExtractColorNodes(Node *node, List **colorList)
{
	bool walkerResult = false;
	if (node == NULL)
	{
		return false;
	}

	if (IsA(node, Const))
	{
		Const *val = (Const *)  node;

		if (val->consttype == TypeOid(2200, "color"))
		{
			*colorList = lappend(*colorList, val);
		}
	}
	else
	{
		walkerResult = expression_tree_walker(node, ExtractColorNodes,
				colorList);
	}

	return walkerResult;
}


static Oid
TypeOid(Oid schemaId, const char *typeName)
{
	Oid typeOid;

	typeOid = GetSysCacheOid2(TYPENAMENSP, Anum_pg_type_oid, 
				 PointerGetDatum(typeName),
				 ObjectIdGetDatum(schemaId));

	return typeOid;
}



Datum
color_in(PG_FUNCTION_ARGS)
{
    char *str = PG_GETARG_CSTRING(0);

    PG_RETURN_COLOR(color_from_str(str));
}

Datum
color_out(PG_FUNCTION_ARGS)
{
  color *c = (color *) PG_GETARG_COLOR(0);

  PG_RETURN_CSTRING(color_to_str(c));
}


Datum
color_recv(PG_FUNCTION_ARGS)
{
	StringInfo	buf = (StringInfo) PG_GETARG_POINTER(0);

	color *result = palloc0(sizeof(color));
	result->r = pq_getmsgint64(buf);
	result->g = pq_getmsgint64(buf);
	result->b = pq_getmsgint64(buf);

	PG_RETURN_COLOR(result);
}

Datum
color_send(PG_FUNCTION_ARGS)
{
	color *a = PG_GETARG_COLOR(0);
	StringInfoData buf;

	pq_begintypsend(&buf);

	pq_sendint8(&buf, a->r);
	pq_sendint8(&buf, a->g);
	pq_sendint8(&buf, a->b);

	PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

Datum
rgb_distance(PG_FUNCTION_ARGS)
{
  color *c1 = (color *) PG_GETARG_COLOR(0);
  color *c2 = (color *) PG_GETARG_COLOR(1);

  if (log_min_messages <= DEBUG5 && 
      client_min_messages <= DEBUG5)
  {
  	PgColorExtendedNode *extendedNode1 = palloc(sizeof(PgColorExtendedNode));
  	PgColorExtendedNode *extendedNode2 = palloc(sizeof(PgColorExtendedNode));

  	extendedNode1->extensible.extnodename = ExtendedNodeName;
  	extendedNode1->extensible.type = T_ExtensibleNode;
  	extendedNode1->interceptedColor = c1;

  	extendedNode2->extensible.extnodename = ExtendedNodeName;
  	extendedNode2->extensible.type = T_ExtensibleNode;
  	extendedNode2->interceptedColor = c2;
	
  	elog(DEBUG4, "left arg:  %s", nodeToString(extendedNode1));
  	elog(DEBUG4, "right arg: %s", nodeToString(extendedNode2));
  	elog(DEBUG4, "left arg and right are equal: %d", equal(extendedNode1, extendedNode2));
  }
  double d1 = (double)c1->r - c2->r;
  double d2 = (double)c1->g - c2->g;
  double d3 = (double)c1->b - c2->b;


  PG_RETURN_FLOAT8(sqrt(d1 * d1 + d2 * d2 + d3 * d3));
}



static inline
color * color_from_str(char *str)
{
	color *c = palloc0(sizeof(color));
	char *endptr = NULL;
	char *cur = str;
	
	if (cur[0] != '(')
		elog(ERROR, "expected '(' at position 0");

	cur++;
	c->r = strtol(cur, &endptr, 10);
	if (cur == endptr)
		elog(ERROR, "expected number at position 1");
	if (endptr[0] != ',')
		elog(ERROR, "expected ',' at position " INT64_FORMAT, endptr - str);

	cur = endptr + 1;
	c->g = strtoll(cur, &endptr, 10);

	if (cur == endptr)
		elog(ERROR, "expected number at position 2");
	if (endptr[0] != ',')
		elog(ERROR, "expected ',' at position " INT64_FORMAT, endptr - str);

	cur = endptr + 1;
	c->b = strtoll(cur, &endptr, 10);

	if (endptr[0] != ')')
		elog(ERROR, "expected ')' at position " INT64_FORMAT, endptr - str);
	if (endptr[1] != '\0')
		elog(ERROR, "unexpected character at position " INT64_FORMAT, 1 + (endptr - str));
	return c;
}

static inline
char *color_to_str(color *c)
{
	char *s = psprintf("(%d,%d,%d)", c->r, c->g, c->b);
 	
	return s;
}

Datum
color_eq(PG_FUNCTION_ARGS)
{
  color *c1 = PG_GETARG_COLOR(0);
  color *c2 = PG_GETARG_COLOR(1);

  return c1->r == c2->r && c1->g == c2->g && c1->b == c2->b;
}

Datum
color_ne(PG_FUNCTION_ARGS)
{
  color *c1 = (color *) PG_GETARG_COLOR(0);
  color *c2 = (color *) PG_GETARG_COLOR(1);

  return c1->r != c2->r || c1->g != c2->g || c1->b != c2->b;
}

Datum
color_cmp(PG_FUNCTION_ARGS)
{
  color *c1 = (color *) PG_GETARG_COLOR(0);
  color *c2 = (color *) PG_GETARG_COLOR(1);

  if (c1 == NULL)
	  return 1;

  if (c2 == NULL)
	  return -1;

  if (c1->r > c2->r)
	  return 1;
  else  if (c1->r < c2->r)
	  return -1;

  if (c1->g > c2->g)
	  return 1;
  else  if (c1->g < c2->g)
	  return -1;

  if (c1->b > c2->b)
	  return 1;
  else  if (c1->b < c2->b)
	  return -1;

  return 0;
}


Datum
color_lt(PG_FUNCTION_ARGS)
{
  color *c1 = (color *) PG_GETARG_COLOR(0);
  color *c2 = (color *) PG_GETARG_COLOR(1);

  if (c1->r < c2->r)
	  return 1;
  else if (c1->r > c2->r)
	  return 0;

  if (c1->g < c2->g)
	  return 1;
  else if (c1->g > c2->g)
	  return 0;

  if (c1->b < c2->b)
	  return 1;
  else if (c1->b > c2->b)
	  return 0;

  return 0;
}


Datum
color_le(PG_FUNCTION_ARGS)
{
  color *c1 = (color *) PG_GETARG_COLOR(0);
  color *c2 = (color *) PG_GETARG_COLOR(1);

  if (c1->r < c2->r)
	  return 1;
  else if (c1->r > c2->r)
	  return 0;

  if (c1->g < c2->g)
	  return 1;
  else if (c1->g > c2->g)
	  return 0;

  if (c1->b < c2->b)
	  return 1;
  else if (c1->b > c2->b)
	  return 0;

  return 1;
}

Datum
color_gt(PG_FUNCTION_ARGS)
{
  color *c1 = (color *) PG_GETARG_COLOR(0);
  color *c2 = (color *) PG_GETARG_COLOR(1);

  if (c1->r > c2->r)
	  return 1;
  else if (c1->r < c2->r)
	  return 0;

  if (c1->g > c2->g)
	  return 1;
  else if (c1->g < c2->g)
	  return 0;

  if (c1->b > c2->b)
	  return 1;
  else if (c1->b < c2->b)
	  return 0;

	 return 0;
}

Datum
color_ge(PG_FUNCTION_ARGS)
{
  color *c1 = (color *) PG_GETARG_COLOR(0);
  color *c2 = (color *) PG_GETARG_COLOR(1);

  if (c1->r > c2->r)
	  return 1;
  else if (c1->r < c2->r)
	  return 0;

  if (c1->g > c2->g)
	  return 1;
  else if (c1->g < c2->g)
	  return 0;

  if (c1->b > c2->b)
	  return 1;
  else if (c1->b < c2->b)
	return 0;

	 return 1;
}

static void
PgColorBeginScan(CustomScanState *node, EState *estate, int eflags)
{
#if PG_VERSION_NUM >= 120000
	ExecInitResultSlot(&node->ss.ps, &TTSOpsMinimalTuple);
#endif
}

static TupleTableSlot *
PgColorExecScan(CustomScanState *node)
{
	PgColorScanState *scanState = (PgColorScanState *) node;
	TupleTableSlot *resultSlot = NULL;

	if (!scanState->finishedScan)
	{
		/*TODO: standardExecutor_Run*/
		DestReceiver *tupleStoreDestReceiever = CreateDestReceiver(DestTuplestore);
		EState *executorState = ScanStateGetExecutorState(scanState);
		ParamListInfo paramListInfo = executorState->es_param_list_info;
		QueryEnvironment *queryEnv = create_queryEnv();
		ScanDirection scanDirection = ForwardScanDirection;
		bool randomAccess = true;
		bool interTransactions = false;

		scanState->tuplestorestate =
			tuplestore_begin_heap(randomAccess, interTransactions, work_mem);

		/*
		 * Use the tupleStore provided by the scanState because it is shared accross
		 * the other task executions and the adaptive executor.
		 */
		SetTuplestoreDestReceiverParams(tupleStoreDestReceiever,
										scanState->tuplestorestate,
										CurrentMemoryContext, false);

		/* Create a QueryDesc for the query */
		QueryDesc *queryDesc = CreateQueryDesc(scanState->plannedStatement
											, "",
									GetActiveSnapshot(), InvalidSnapshot,
									tupleStoreDestReceiever, paramListInfo,
									queryEnv, 0);

		standard_ExecutorStart(queryDesc,0);
		standard_ExecutorRun(queryDesc, scanDirection, 0L, true);
		standard_ExecutorFinish(queryDesc);

		standard_ExecutorEnd(queryDesc);
		UnregisterSnapshot(GetActiveSnapshot());

		scanState->finishedScan = true;
	}

	resultSlot = ReturnTupleFromTuplestore(scanState);


	return resultSlot;
}


static TupleTableSlot *
ReturnTupleFromTuplestore(PgColorScanState *scanState)
{
	Tuplestorestate *tupleStore = scanState->tuplestorestate;
	TupleTableSlot *resultSlot = NULL;
	EState *executorState = NULL;
	ScanDirection scanDirection = NoMovementScanDirection;
	bool forwardScanDirection = true;

	if (tupleStore == NULL)
	{
		return NULL;
	}

	executorState = ScanStateGetExecutorState(scanState);
	scanDirection = executorState->es_direction;
	Assert(ScanDirectionIsValid(scanDirection));

	if (ScanDirectionIsBackward(scanDirection))
	{
		forwardScanDirection = false;
	}

	resultSlot = scanState->customScanState.ss.ps.ps_ResultTupleSlot;
	tuplestore_gettupleslot(tupleStore, forwardScanDirection, false, resultSlot);

	return resultSlot;
}



static void
PgColorEndScan(CustomScanState *node)
{
	PgColorScanState *scanState = (PgColorScanState *) node;

	if (scanState->tuplestorestate)
	{
		tuplestore_end(scanState->tuplestorestate);
		scanState->tuplestorestate = NULL;
	}
	
          if (log_min_messages <= DEBUG4 ||
          client_min_messages <= DEBUG4)
        {
                elog(DEBUG4, "Executor intercepted the color %s", nodeToString(scanState->color));

	}

	
        	ColorQueryStatsExecutorsEntry(scanState->queryId, scanState->color->interceptedColor);
	
}

static void
PgColorReScan(CustomScanState *node)
{


}

static Node *
PgColorCreateScan(CustomScan *scan)
{
	PgColorScanState *scanState = palloc0(sizeof(PgColorScanState));
	PgColorExtendedNode *colorData = GetPgColorExtendedNode(scan);
	scanState->customScanState.ss.ps.type = T_CustomScanState;
	scanState->color = colorData;
	scanState->plannedStatement = GetOriginalPlan(scan);
	scanState->queryId = scanState->plannedStatement->queryId;
	scanState->customScanState.methods = &PgColorCustomExecMethods;

	return (Node *) scanState;
}


static PgColorExtendedNode *
GetPgColorExtendedNode(CustomScan *customScan)
{
	Node *node = NULL;
	PgColorExtendedNode *color = NULL;

	Assert(list_length(customScan->custom_private) == 1);

	node = (Node *) linitial(customScan->custom_private);

	color = (PgColorExtendedNode *) node;

	return color;
}

static PlannedStmt *
GetOriginalPlan(CustomScan *customScan)
{
	Node *node = NULL;
	PlannedStmt *plan = NULL;

	node = (Node *) linitial(customScan->custom_plans);

	plan = (PlannedStmt *) node;

	return plan;
}


static PlannedStmt *
FinalizePlan(PlannedStmt *localPlan, color *interceptedColor)
{
	PlannedStmt *finalPlan = NULL;
	CustomScan *customScan = makeNode(CustomScan);
	PgColorExtendedNode *interceptedColorData = NULL;

	RangeTblEntry *remoteScanRangeTableEntry = NULL;

	customScan->methods = &PgColorCustomScanMethods;

	interceptedColorData = palloc(sizeof(PgColorExtendedNode));
	interceptedColorData->extensible.extnodename = ExtendedNodeName;
	interceptedColorData->extensible.type = T_ExtensibleNode;
	interceptedColorData->interceptedColor = interceptedColor;

	customScan->custom_private = list_make1(interceptedColorData);
	customScan->custom_plans = list_make1(localPlan);
	customScan->flags = CUSTOMPATH_SUPPORT_BACKWARD_SCAN;


	/* we will have custom scan range table entry as the first one in the list */
	int customScanRangeTableIndex = 1;
	ListCell *targetEntryCell = NULL;
	List *targetList = NIL;
	List *columnNameList = NIL;

	/* build a targetlist to read from the custom scan output */
	foreach(targetEntryCell, localPlan->planTree->targetlist)
	{
		TargetEntry *targetEntry = lfirst(targetEntryCell);
		TargetEntry *newTargetEntry = NULL;
		Var *newVar = NULL;
		Value *columnName = NULL;

		Assert(IsA(targetEntry, TargetEntry));

		/*
		 * This is unlikely to be hit because we would not need resjunk stuff
		 * at the toplevel of a router query - all things needing it have been
		 * pushed down.
		 */
		if (targetEntry->resjunk)
		{
			continue;
		}

		/* build target entry pointing to remote scan range table entry */
		newVar = makeVarFromTargetEntry(customScanRangeTableIndex, targetEntry);

		newTargetEntry = flatCopyTargetEntry(targetEntry);
		newTargetEntry->expr = (Expr *) newVar;
		targetList = lappend(targetList, newTargetEntry);

		columnName = makeString(targetEntry->resname);
		columnNameList = lappend(columnNameList, columnName);
	}

	customScan->scan.plan.targetlist = targetList;

	finalPlan = makeNode(PlannedStmt);
	finalPlan->planTree = (Plan *) customScan;

		finalPlan->canSetTag = true;
	finalPlan->relationOids = NIL;

	finalPlan->queryId = localPlan->queryId;
	finalPlan->utilityStmt = localPlan->utilityStmt;
	finalPlan->commandType = localPlan->commandType;
	finalPlan->hasReturning = localPlan->hasReturning;


	remoteScanRangeTableEntry = RemoteScanRangeTableEntry(columnNameList);
	finalPlan->rtable = list_make1(remoteScanRangeTableEntry);


	return finalPlan;
}



static RangeTblEntry *
RemoteScanRangeTableEntry(List *columnNameList)
{
	RangeTblEntry *remoteScanRangeTableEntry = makeNode(RangeTblEntry);

	/* we use RTE_VALUES for custom scan because we can't look up relation */
	remoteScanRangeTableEntry->rtekind = RTE_VALUES;
	remoteScanRangeTableEntry->eref = makeAlias("remote_scan", columnNameList);
	remoteScanRangeTableEntry->inh = false;
	remoteScanRangeTableEntry->inFromCl = true;

	return remoteScanRangeTableEntry;
}

static EState *
ScanStateGetExecutorState(PgColorScanState *scanState)
{
	return scanState->customScanState.ss.ps.state;
}


void
ColorQueryStatsExecutorsEntry(uint64 queryId, color *color)
{
	volatile QueryStatsEntry *e;

	QueryStatsHashKey key;
	QueryStatsEntry *entry;

	/* Safety check... */
	if (!queryStats || !queryStatsHash)
	{
		return;
	}

	/* Set up key for hashtable search */
	key.userid = GetUserId();
	key.dbid = MyDatabaseId;
	key.queryid = queryId;
	memset(key.color, 0, MAX_KEY_LENGTH);
	if (color != NULL)
	{
		StringInfo str = makeStringInfo();

		appendStringInfo(str, "(%d,%d,%d)", color->r, color->g, color->b);

		strlcpy(key.color, str->data, MAX_KEY_LENGTH);
	}

	/* Lookup the hash table entry with shared lock. */
	LWLockAcquire(queryStats->lock, LW_SHARED);

	entry = (QueryStatsEntry *) hash_search(queryStatsHash, &key, HASH_FIND, NULL);

	/* Create new entry, if not present */
	if (!entry)
	{
		/* Need exclusive lock to make a new hashtable entry - promote */
		LWLockRelease(queryStats->lock);
		LWLockAcquire(queryStats->lock, LW_EXCLUSIVE);

		/* OK to create a new hashtable entry */
		entry = ColorQueryStatsEntryAlloc(&key, false);
	}

	/*
	 * Grab the spinlock while updating the counters (see comment about
	 * locking rules at the head of the pg_stat_statements file)
	 */
	e = (volatile QueryStatsEntry *) entry;

	SpinLockAcquire(&e->mutex);

	/* "Unstick" entry if it was previously sticky */
	if (e->calls == 0)
	{
		e->usage = USAGE_INIT;
	}

	e->calls += 1;

	SpinLockRelease(&e->mutex);

	LWLockRelease(queryStats->lock);
}

static void
ColorQueryStatsShmemStartup(void)
{
	bool found;
	HASHCTL info;
	FILE *file;
	int i;
	uint32 header;
	int32 num;
	QueryStatsEntry *buffer = NULL;

	if (prev_shmem_startup_hook)
	{
		prev_shmem_startup_hook();
	}

	/* Create or attach to the shared memory state */
	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	/* global access lock */
	queryStats = ShmemInitStruct(STATS_SHARED_MEM_NAME,
								 sizeof(QueryStatsSharedState),
								 &found);

	if (!found)
	{
		/* First time through ... */
		queryStats->lock = &(GetNamedLWLockTranche(STATS_SHARED_MEM_NAME))->lock;
	}

	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(QueryStatsHashKey);
	info.entrysize = sizeof(QueryStatsEntry);
	info.hash = ColorQuerysStatsHashFn;
	info.match = ColorQuerysStatsMatchFn;

	/* allocate stats shared memory hash */
	queryStatsHash = ShmemInitHash("color_query_stats hash",
								   ColorStatsMax, ColorStatsMax,
								   &info,
								   HASH_ELEM | HASH_FUNCTION | HASH_COMPARE);

	LWLockRelease(AddinShmemInitLock);

	if (!IsUnderPostmaster)
	{
		on_shmem_exit(ColorQueryStatsShmemShutdown, (Datum) 0);
	}

	/*
	 * Done if some other process already completed our initialization.
	 */
	if (found)
	{
		return;
	}

	/* Load stat file, don't care about locking */
	file = AllocateFile(COLOR_STATS_DUMP_FILE, PG_BINARY_R);
	if (file == NULL)
	{
		if (errno == ENOENT)
		{
			return;         /* ignore not-found error */
		}
		goto error;
	}

	/* check is header is valid */
	if (fread(&header, sizeof(uint32), 1, file) != 1 ||
		header != COLOR_QUERY_STATS_FILE_HEADER)
	{
		goto error;
	}

	/* get number of entries */
	if (fread(&num, sizeof(int32), 1, file) != 1)
	{
		goto error;
	}

	for (i = 0; i < num; i++)
	{
		QueryStatsEntry temp;
		QueryStatsEntry *entry;

		if (fread(&temp, sizeof(QueryStatsEntry), 1, file) != 1)
		{
			goto error;
		}

		/* Skip loading "sticky" entries */
		if (temp.calls == 0)
		{
			continue;
		}

		entry = ColorQueryStatsEntryAlloc(&temp.key, false);

		/* copy in the actual stats */
		entry->calls = temp.calls;
		entry->usage = temp.usage;

		/* don't initialize spinlock, already done */
	}

	FreeFile(file);

	/*
	 * Remove the file so it's not included in backups/replication slaves,
	 * etc. A new file will be written on next shutdown.
	 */
	unlink(COLOR_STATS_DUMP_FILE);

	return;

error:
	ereport(LOG,
			(errcode_for_file_access(),
			 errmsg("could not read color_query_stats file \"%s\": %m",
					COLOR_STATS_DUMP_FILE)));
	if (buffer)
	{
		pfree(buffer);
	}
	if (file)
	{
		FreeFile(file);
	}

	/* delete bogus file, don't care of errors in this case */
	unlink(COLOR_STATS_DUMP_FILE);
}

/*
 * Allocate a new hashtable entry.
 * caller must hold an exclusive lock on queryStats->lock
 */
static QueryStatsEntry *
ColorQueryStatsEntryAlloc(QueryStatsHashKey *key, bool sticky)
{
	QueryStatsEntry *entry;
	bool found;

	/* Make space if needed */
	while (hash_get_num_entries(queryStatsHash) >= ColorStatsMax)
	{
		ColorQueryStatsEntryDealloc();
	}

	/* Find or create an entry with desired hash code */
	entry = (QueryStatsEntry *) hash_search(queryStatsHash, key, HASH_ENTER, &found);

	if (!found)
	{
		/* New entry, initialize it */

		/* set the appropriate initial usage count */
		entry->usage = sticky ? queryStats->cur_median_usage : USAGE_INIT;

		/* re-initialize the mutex each time ... we assume no one using it */
		SpinLockInit(&entry->mutex);
	}

	entry->calls = 0;
	entry->usage = (0.0);

	return entry;
}


/*
 * entry_cmp is qsort comparator for sorting into increasing usage order
 */
static int
entry_cmp(const void *lhs, const void *rhs)
{
	double l_usage = (*(QueryStatsEntry *const *) lhs)->usage;
	double r_usage = (*(QueryStatsEntry *const *) rhs)->usage;

	if (l_usage < r_usage)
	{
		return -1;
	}
	else if (l_usage > r_usage)
	{
		return +1;
	}
	else
	{
		return 0;
	}
}


/*
 * ColorQueryStatsEntryDealloc deallocates least used entries.
 * Caller must hold an exclusive lock on queryStats->lock.
 */
static void
ColorQueryStatsEntryDealloc(void)
{
	HASH_SEQ_STATUS hash_seq;
	QueryStatsEntry **entries;
	QueryStatsEntry *entry;
	int nvictims;
	int i;

	/*
	 * Sort entries by usage and deallocate USAGE_DEALLOC_PERCENT of them.
	 * While we're scanning the table, apply the decay factor to the usage
	 * values.
	 */
	entries = palloc(hash_get_num_entries(queryStatsHash) * sizeof(QueryStatsEntry *));

	i = 0;
	hash_seq_init(&hash_seq, queryStatsHash);
	while ((entry = hash_seq_search(&hash_seq)) != NULL)
	{
		entries[i++] = entry;

		/* "Sticky" entries get a different usage decay rate. */
		if (entry->calls == 0)
		{
			entry->usage *= STICKY_DECREASE_FACTOR;
		}
		else
		{
			entry->usage *= USAGE_DECREASE_FACTOR;
		}
	}

	qsort(entries, i, sizeof(QueryStatsEntry *), entry_cmp);

	if (i > 0)
	{
		/* Record the (approximate) median usage */
		queryStats->cur_median_usage = entries[i / 2]->usage;
	}

	nvictims = Max(10, i * USAGE_DEALLOC_PERCENT / 100);
	nvictims = Min(nvictims, i);

	for (i = 0; i < nvictims; i++)
	{
		hash_search(queryStatsHash, &entries[i]->key, HASH_REMOVE, NULL);
	}

	pfree(entries);
}


/*
 */
static void
ColorQueryStatsEntryReset(void)
{
	HASH_SEQ_STATUS hash_seq;
	QueryStatsEntry *entry;

	LWLockAcquire(queryStats->lock, LW_EXCLUSIVE);

	hash_seq_init(&hash_seq, queryStatsHash);
	while ((entry = hash_seq_search(&hash_seq)) != NULL)
	{
		hash_search(queryStatsHash, &entry->key, HASH_REMOVE, NULL);
	}

	LWLockRelease(queryStats->lock);
}


/*
 */
#include "catalog/pg_collation.h"
static uint32
ColorQuerysStatsHashFn(const void *key, Size keysize)
{
	const QueryStatsHashKey *k = (const QueryStatsHashKey *) key;

	if (k->color[0] != '\0')
	{
		//Datum colorHash  = DirectFunctionCall1Coll(hashtext, DEFAULT_COLLATION_OID, CStringGetDatum(k->color));

		return hash_uint32((uint32) k->userid) ^
			   hash_uint32((uint32) k->dbid) ^
			   hash_any((const unsigned char *) &(k->queryid), sizeof(uint64));

	}
	else
	{
		return 0;
	}
}


/*
 * ColorQuerysStatsMatchFn compares two keys - zero means match.
 * See definition of HashCompareFunc in hsearch.h for more info.
 */
static int
ColorQuerysStatsMatchFn(const void *key1, const void *key2, Size keysize)
{
	const QueryStatsHashKey *k1 = (const QueryStatsHashKey *) key1;
	const QueryStatsHashKey *k2 = (const QueryStatsHashKey *) key2;

	if (k1->userid == k2->userid &&
		k1->dbid == k2->dbid &&
		k1->queryid == k2->queryid &&
		strncmp(k1->color, k2->color, 256) == 0)
	{
		return 0;
	}
	else
	{
		return 1;
	}
}


/*
 * ExistingStatsHashFn calculates and returns hash value for ExistingStatsHashKey
 */
static uint32
ExistingStatsHashFn(const void *key, Size keysize)
{
	const ExistingStatsHashKey *k = (const ExistingStatsHashKey *) key;

	return hash_uint32((uint32) k->userid) ^
		   hash_uint32((uint32) k->dbid) ^
		   hash_any((const unsigned char *) &(k->queryid), sizeof(uint64));
}


/*
 * ExistingStatsMatchFn compares two keys of type ExistingStatsHashKey - zero
 * means match. See definition of HashCompareFunc in hsearch.h for more info.
 */
static int
ExistingStatsMatchFn(const void *key1, const void *key2, Size keysize)
{
	const ExistingStatsHashKey *k1 = (const ExistingStatsHashKey *) key1;
	const ExistingStatsHashKey *k2 = (const ExistingStatsHashKey *) key2;


	if (k1->userid == k2->userid &&
		k1->dbid == k2->dbid &&
		k1->queryid == k2->queryid)
	{
		return 0;
	}

	return 1;
}


/*
 * Reset statistics.
 */
Datum
color_stat_statements_reset(PG_FUNCTION_ARGS)
{
	ColorQueryStatsEntryReset();
	PG_RETURN_VOID();
}

/*
 * ColorQueryStatsSynchronizeEntries removes all entries in queryStats hash
 * that does not have matching queryId in pg_stat_statements.
 *
 * A function called inside (ColorQueryStatsRemoveExpiredEntries) acquires
 * an exclusive lock on queryStats->lock.
 */
static void
ColorQueryStatsSynchronizeEntries(void)
{
	HTAB *existingQueryIdHash = BuildExistingQueryIdHash();
	if (existingQueryIdHash != NULL)
	{
		ColorQueryStatsRemoveExpiredEntries(existingQueryIdHash);
		hash_destroy(existingQueryIdHash);
	}
}



/*
 * color_query_stats returns query stats kept in memory.
 */
Datum
color_query_stats(PG_FUNCTION_ARGS)
{
	TupleDesc tupdesc;
	Tuplestorestate *tupstore;
	HASH_SEQ_STATUS hash_seq;
	QueryStatsEntry *entry;
	Oid currentUserId = GetUserId();
	bool canSeeStats = superuser();

	if (!queryStats)
	{
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("color_query_stats: shared memory not initialized")));
	}

	tupstore = SetupTuplestore(fcinfo, &tupdesc);


	/* exclusive lock on queryStats->lock is acquired and released inside the function */
	ColorQueryStatsSynchronizeEntries();

	LWLockAcquire(queryStats->lock, LW_SHARED);

	hash_seq_init(&hash_seq, queryStatsHash);
	while ((entry = hash_seq_search(&hash_seq)) != NULL)
	{
		Datum values[COLOR_STAT_STATEMENTS_COLS];
		bool nulls[COLOR_STAT_STATEMENTS_COLS];

		/* following vars are to keep data for processing after spinlock release */
		uint64 queryid = 0;
		Oid userid = InvalidOid;
		Oid dbid = InvalidOid;
		char color[MAX_KEY_LENGTH];
		int64 calls = 0;

		memset(values, 0, sizeof(values));
		memset(nulls, 0, sizeof(nulls));
		memset(color, 0, MAX_KEY_LENGTH);

		SpinLockAcquire(&entry->mutex);

		/*
		 * Skip entry if unexecuted (ie, it's a pending "sticky" entry) or
		 * the user does not have permission to view it.
		 */
		if (entry->calls == 0 || !(currentUserId == entry->key.userid || canSeeStats))
		{
			SpinLockRelease(&entry->mutex);
			continue;
		}

		queryid = entry->key.queryid;
		userid = entry->key.userid;
		dbid = entry->key.dbid;

		memcpy(color, entry->key.color, MAX_KEY_LENGTH);

		calls = entry->calls;

		SpinLockRelease(&entry->mutex);

		values[COLOR_STAT_STATAMENTS_QUERY_ID] = UInt64GetDatum(queryid);
		values[COLOR_STAT_STATAMENTS_USER_ID] = ObjectIdGetDatum(userid);
		values[COLOR_STAT_STATAMENTS_DB_ID] = ObjectIdGetDatum(dbid);

		if (color[0] != '\0')
		{
			values[COLOR_STAT_STATAMENTS_COLOR] = CStringGetTextDatum(
				color);
		}
		else
		{
			nulls[COLOR_STAT_STATAMENTS_COLOR] = true;
		}

		values[COLOR_STAT_STATAMENTS_CALLS] = Int64GetDatumFast(calls);

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}

	LWLockRelease(queryStats->lock);

	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupstore);

	return (Datum) 0;
}

static Tuplestorestate *
SetupTuplestore(FunctionCallInfo fcinfo, TupleDesc *tupleDescriptor)
{
	ReturnSetInfo *resultSet = CheckTuplestoreReturn(fcinfo, tupleDescriptor);
	MemoryContext perQueryContext = resultSet->econtext->ecxt_per_query_memory;

	MemoryContext oldContext = MemoryContextSwitchTo(perQueryContext);
	Tuplestorestate *tupstore = tuplestore_begin_heap(true, false, work_mem);
	resultSet->returnMode = SFRM_Materialize;
	resultSet->setResult = tupstore;
	resultSet->setDesc = *tupleDescriptor;
	MemoryContextSwitchTo(oldContext);

	return tupstore;
}

/*
 * CheckTuplestoreReturn checks if a tuplestore can be returned in the callsite
 * of the UDF.
 */
static ReturnSetInfo *
CheckTuplestoreReturn(FunctionCallInfo fcinfo, TupleDesc *tupdesc)
{
	ReturnSetInfo *returnSetInfo = (ReturnSetInfo *) fcinfo->resultinfo;

	/* check to see if caller supports us returning a tuplestore */
	if (returnSetInfo == NULL || !IsA(returnSetInfo, ReturnSetInfo))
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot " \
						"accept a set")));
	}
	if (!(returnSetInfo->allowedModes & SFRM_Materialize))
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not " \
						"allowed in this context")));
	}
	switch (get_call_result_type(fcinfo, NULL, tupdesc))
	{
		case TYPEFUNC_COMPOSITE:
		{
			/* success */
			break;
		}

		case TYPEFUNC_RECORD:
		{
			/* failed to determine actual type of RECORD */
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("function returning record called in context "
							"that cannot accept type record")));
			break;
		}

		default:
		{
			/* result type isn't composite */
			elog(ERROR, "return type must be a row type");
			break;
		}
	}
	return returnSetInfo;
}




/*
 * BuildExistingQueryIdHash goes over entries in pg_stat_statements and prepare
 * a hash table of queryId's. The function returns null if
 * public.pg_stat_statements(bool) function is not available. Returned hash
 * table is allocated on the CurrentMemoryContext, and caller is responsible
 * for deallocation.
 */
static HTAB *
BuildExistingQueryIdHash(void)
{
	HTAB *queryIdHashTable = NULL;
	const int userIdAttributeNumber = 1;
	const int dbIdAttributeNumber = 2;
	const int queryIdAttributeNumber = 3;
	FmgrInfo *fmgrPGStatStatements = NULL;
	ReturnSetInfo *statStatementsReturnSet = NULL;
	TupleTableSlot *tupleTableSlot = NULL;
	Datum commandTypeDatum = (Datum) 0;
	HASHCTL info;
	int hashFlags = 0;
	int pgColorStatsMax = 0;
	bool missingOK = true;

	Oid pgStatStatementsOid = FunctionOidExtended("public", "pg_stat_statements", 1,
												  missingOK);
	if (!OidIsValid(pgStatStatementsOid))
	{
		return NULL;
	}

	/* fetch pg_stat_statements.max, it is expected to be available, if not bail out */
	pgColorStatsMax = GetPGColorStatsMax();
	if (pgColorStatsMax == 0)
	{
		ereport(DEBUG1, (errmsg("Cannot access pg_stat_statements.max")));
		return NULL;
	}

	fmgrPGStatStatements = (FmgrInfo *) palloc0(sizeof(FmgrInfo));
	commandTypeDatum = BoolGetDatum(false);

	fmgr_info(pgStatStatementsOid, fmgrPGStatStatements);

	statStatementsReturnSet = FunctionCallGetTupleStore1(fmgrPGStatStatements->fn_addr,
														 pgStatStatementsOid,
														 commandTypeDatum);
	tupleTableSlot = MakeSingleTupleTableSlot(statStatementsReturnSet->setDesc,
													&TTSOpsMinimalTuple);

	info.keysize = sizeof(ExistingStatsHashKey);
	info.entrysize = sizeof(ExistingStatsHashKey);
	info.hcxt = CurrentMemoryContext;
	info.hash = ExistingStatsHashFn;
	info.match = ExistingStatsMatchFn;

	hashFlags = (HASH_ELEM | HASH_CONTEXT | HASH_FUNCTION | HASH_COMPARE);

	/*
	 * Allocate more hash slots (twice as much) than necessary to minimize
	 * collisions.
	 */
	queryIdHashTable = hash_create("pg_stats_statements queryId hash",
								   pgColorStatsMax * 2, &info, hashFlags);

	/* iterate over tuples in tuple store, and add queryIds to hash table */
	while (true)
	{
		bool tuplePresent = false;
		bool isNull = false;
		Datum queryIdDatum = 0;
		Datum dbIdDatum = 0;
		Datum userIdDatum = 0;

		tuplePresent = tuplestore_gettupleslot(statStatementsReturnSet->setResult,
											   true,
											   false,
											   tupleTableSlot);

		if (!tuplePresent)
		{
			break;
		}

		userIdDatum = slot_getattr(tupleTableSlot, userIdAttributeNumber, &isNull);
		dbIdDatum = slot_getattr(tupleTableSlot, dbIdAttributeNumber, &isNull);
		queryIdDatum = slot_getattr(tupleTableSlot, queryIdAttributeNumber, &isNull);

		/*
		 * queryId may be returned as NULL when current user is not authorized to see other
		 * users' stats.
		 */
		if (!isNull)
		{
			ExistingStatsHashKey key;
			key.userid = DatumGetInt32(userIdDatum);
			key.dbid = DatumGetInt32(dbIdDatum);
			key.queryid = DatumGetInt64(queryIdDatum);

			hash_search(queryIdHashTable, (void *) &key, HASH_ENTER, NULL);
		}

		ExecClearTuple(tupleTableSlot);
	}

	ExecDropSingleTupleTableSlot(tupleTableSlot);

	tuplestore_end(statStatementsReturnSet->setResult);

	pfree(fmgrPGStatStatements);

	return queryIdHashTable;
}


/*
 * GetPGColorStatsMax returns GUC value pg_stat_statements.max. The
 * function returns 0 if for some reason it can not access
 * pg_stat_statements.max value.
 */
static int
GetPGColorStatsMax(void)
{
	const char *pgssMax;
	const char *name = "pg_stat_statements.max";
	int maxValue = 0;

	pgssMax = GetConfigOption(name, true, false);

	/*
	 * Retrieving pg_stat_statements.max can fail if the extension is loaded
	 * after pgcolor in shared_preload_libraries, or not at all.
	 */
	if (pgssMax)
	{
		maxValue = pg_atoi(pgssMax, 4, 0);
	}

	return maxValue;
}


/*
 * ColorQueryStatsRemoveExpiredEntries iterates over queryStats hash entries
 * and removes entries with keys that do not exists in the provided hash of
 * queryIds.
 *
 * Acquires and releases exclusive lock on queryStats->lock.
 */
static void
ColorQueryStatsRemoveExpiredEntries(HTAB *existingQueryIdHash)
{
	HASH_SEQ_STATUS hash_seq;
	QueryStatsEntry *entry;
	int removedCount = 0;
	bool canSeeStats = superuser();
	Oid currentUserId = GetUserId();


	LWLockAcquire(queryStats->lock, LW_EXCLUSIVE);

	hash_seq_init(&hash_seq, queryStatsHash);
	while ((entry = hash_seq_search(&hash_seq)) != NULL)
	{
		bool found = false;
		ExistingStatsHashKey existingStatsKey = { 0, 0, 0 };

		/*
		 * pg_stat_statements returns NULL in the queryId field for queries
		 * belonging to other users. Those queries are therefore not reflected
		 * in the existingQueryIdHash, but that does not mean that we should
		 * remove them as they are relevant to other users.
		 */
		if (!(currentUserId == entry->key.userid || canSeeStats))
		{
			continue;
		}

		existingStatsKey.userid = entry->key.userid;
		existingStatsKey.dbid = entry->key.dbid;
		existingStatsKey.queryid = entry->key.queryid;

		hash_search(existingQueryIdHash, (void *) &existingStatsKey, HASH_FIND, &found);
		if (!found)
		{
			hash_search(queryStatsHash, &entry->key, HASH_REMOVE, NULL);
			removedCount++;
		}
	}

	LWLockRelease(queryStats->lock);

	if (removedCount > 0)
	{
		elog(DEBUG2, "color_stat_statements removed %d expired entries", removedCount);
	}
}

/*
 * FunctionOidExtended searches for a given function identified by schema,
 * functionName, and argumentCount. It reports error if the function is not
 * found or there are more than one match. If the missingOK parameter is set
 * and there are no matches, then the function returns InvalidOid.
 */
static Oid
FunctionOidExtended(const char *schemaName, const char *functionName, int argumentCount,
					bool missingOK)
{
	FuncCandidateList functionList = NULL;
	Oid functionOid = InvalidOid;

	char *qualifiedFunctionName = quote_qualified_identifier(schemaName, functionName);
	List *qualifiedFunctionNameList = stringToQualifiedNameList(qualifiedFunctionName);
	List *argumentList = NIL;
	const bool findVariadics = false;
	const bool findDefaults = false;

	functionList = FuncnameGetCandidates(qualifiedFunctionNameList, argumentCount,
										 argumentList, findVariadics,
										 findDefaults, true);

	if (functionList == NULL)
	{
		if (missingOK)
		{
			return InvalidOid;
		}

		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FUNCTION),
						errmsg("function \"%s\" does not exist", functionName)));
	}
	else if (functionList->next != NULL)
	{
		ereport(ERROR, (errcode(ERRCODE_AMBIGUOUS_FUNCTION),
						errmsg("more than one function named \"%s\"", functionName)));
	}

	/* get function oid from function list's head */
	functionOid = functionList->oid;

	return functionOid;
}

/*
 * FunctionCallGetTupleStore1 calls the given set-returning PGFunction with the given
 * argument and returns the ResultSetInfo filled by the called function.
 */
static ReturnSetInfo *
FunctionCallGetTupleStore1(PGFunction function, Oid functionId, Datum argument)
{
	LOCAL_FCINFO(fcinfo, 1);
	FmgrInfo flinfo;
	ReturnSetInfo *rsinfo = makeNode(ReturnSetInfo);
	EState *estate = CreateExecutorState();
	rsinfo->econtext = GetPerTupleExprContext(estate);
	rsinfo->allowedModes = SFRM_Materialize;

	fmgr_info(functionId, &flinfo);
	InitFunctionCallInfoData(*fcinfo, &flinfo, 1, InvalidOid, NULL, (Node *) rsinfo);

	fcSetArg(fcinfo, 0, argument);

	(*function)(fcinfo);

	return rsinfo;
}

static Size
ColorQueryStatsSharedMemSize(void)
{
	Size size;

	Assert(ColorStatsMax >= 0);

	size = MAXALIGN(sizeof(QueryStatsSharedState));
	size = add_size(size, hash_estimate_size(ColorStatsMax, sizeof(QueryStatsEntry)));

	return size;
}



static void
ColorQueryStatsShmemShutdown(int code, Datum arg)
{
	FILE *file;
	HASH_SEQ_STATUS hash_seq;
	int32 num_entries;
	QueryStatsEntry *entry;

	/* Don't try to dump during a crash. */
	if (code)
	{
		return;
	}

	if (!queryStats)
	{
		return;
	}

	file = AllocateFile(COLOR_STATS_DUMP_FILE ".tmp", PG_BINARY_W);
	if (file == NULL)
	{
		goto error;
	}

	if (fwrite(&COLOR_QUERY_STATS_FILE_HEADER, sizeof(uint32), 1, file) != 1)
	{
		goto error;
	}

	num_entries = hash_get_num_entries(queryStatsHash);

	if (fwrite(&num_entries, sizeof(int32), 1, file) != 1)
	{
		goto error;
	}

	hash_seq_init(&hash_seq, queryStatsHash);
	while ((entry = hash_seq_search(&hash_seq)) != NULL)
	{
		if (fwrite(entry, sizeof(QueryStatsEntry), 1, file) != 1)
		{
			/* note: we assume hash_seq_term won't change errno */
			hash_seq_term(&hash_seq);
			goto error;
		}
	}

	if (FreeFile(file))
	{
		file = NULL;
		goto error;
	}

	/*
	 * Rename file inplace
	 */
	if (rename(COLOR_STATS_DUMP_FILE ".tmp", COLOR_STATS_DUMP_FILE) != 0)
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not rename color_query_stats file \"%s\": %m",
						COLOR_STATS_DUMP_FILE ".tmp")));
	}

	return;

error:
	ereport(LOG,
			(errcode_for_file_access(),
			 errmsg("could not read color_query_stats file \"%s\": %m",
					COLOR_STATS_DUMP_FILE)));

	if (file)
	{
		FreeFile(file);
	}
	unlink(COLOR_STATS_DUMP_FILE);
}
