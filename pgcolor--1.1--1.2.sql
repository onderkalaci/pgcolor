CREATE OR REPLACE FUNCTION  color_query_stats(OUT queryid bigint,
                                                                                         OUT userid oid,
                                                                                         OUT dbid oid,
                                                                                         OUT color text,
                                                                                         OUT calls bigint)
RETURNS SETOF record
LANGUAGE C STRICT
AS 'pgcolor', $$color_query_stats$$;



CREATE FUNCTION color_stat_statements_reset()
RETURNS void
LANGUAGE C STRICT
AS 'pgcolor', $$color_stat_statements_reset$$;
