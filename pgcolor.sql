-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pgcolor" to load this file. \quit


CREATE OR REPLACE FUNCTION color_in(cstring)
RETURNS color
AS '$libdir/pgcolor'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION color_out(color)
RETURNS cstring
AS '$libdir/pgcolor'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION color_recv(internal)
RETURNS color
AS 'pgcolor', 'color_recv'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION color_send(color)
RETURNS bytea
AS 'pgcolor', 'color_send'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;


CREATE TYPE color (
  INPUT = color_in,
  OUTPUT = color_out,
  RECEIVE = color_recv,
  SEND = color_send,
  internallength = 24

);
COMMENT ON TYPE color IS 'Color data type for PostgreSQL';


UPDATE pg_type SET typsend='color_send', typreceive='color_recv' WHERE typname='color';


CREATE FUNCTION rgb_distance(color, color)
RETURNS float
AS 'pgcolor', 'rgb_distance'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE OPERATOR <-> (
	LEFTARG = color, RIGHTARG = color, PROCEDURE = rgb_distance,
	COMMUTATOR = '<->'
);


