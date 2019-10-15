
#include "postgres.h"
#include "miscadmin.h"

#include "catalog/pg_type.h"
#include "utils/elog.h"
#include "utils/palloc.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/syscache.h"
#include "libpq/pqformat.h"
#include "nodes/makefuncs.h"
#include "nodes/extensible.h"
#include "nodes/readfuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/planner.h"
#include "executor/tstoreReceiver.h"
#include "utils/snapmgr.h"

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
