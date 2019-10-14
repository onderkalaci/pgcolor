
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
}

Datum
color_out(PG_FUNCTION_ARGS)
{
}


Datum
color_recv(PG_FUNCTION_ARGS)
{
}

Datum
color_send(PG_FUNCTION_ARGS)
{
}

Datum
rgb_distance(PG_FUNCTION_ARGS)
{
}

