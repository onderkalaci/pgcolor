
#include "postgres.h"
#include "miscadmin.h"

#include "utils/elog.h"
#include "utils/palloc.h"
#include "utils/builtins.h"
#include "libpq/pqformat.h"
#include "nodes/makefuncs.h"

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


#define DatumGetColor(X)	 ((color *) DatumGetPointer(X))
#define ColorGetDatum(X)	 PointerGetDatum(X)
#define PG_GETARG_COLOR(n)	 DatumGetColor(PG_GETARG_DATUM(n))
#define PG_RETURN_COLOR(x)	 return ColorGetDatum(x)

static void _PG_init();
static color * color_from_str(char *str);
static char *color_to_str(color *c);

Datum color_in(PG_FUNCTION_ARGS);
Datum color_out(PG_FUNCTION_ARGS);
Datum color_recv(PG_FUNCTION_ARGS);
Datum color_send(PG_FUNCTION_ARGS);
Datum rgb_distance(PG_FUNCTION_ARGS);


PG_FUNCTION_INFO_V1(color_in);
PG_FUNCTION_INFO_V1(color_out);
PG_FUNCTION_INFO_V1(color_recv);
PG_FUNCTION_INFO_V1(color_send);
PG_FUNCTION_INFO_V1(rgb_distance);

void
_PG_init(void)
{

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
