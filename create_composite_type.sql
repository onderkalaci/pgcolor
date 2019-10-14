-- finding distance between two colors is a common
-- problem. If you don't believe me, check this e-mail
-- on Postgres hackers: https://www.postgresql.org/message-id/93de83770909032246y12c557c3r37278eec92057b9e@mail.gmail.com

-- There are various research going on this topic as well
-- https://www.ingentaconnect.com/content/ist/ei/2016/00002016/00000020/art00036

-- So, in simple words, the problem is calculating the distance between two colors (a.k.a, distance)
-- We choose one of the simplest methods of this calculation, just for simplicity


-- Euclidian distance is defined as follows
distance(color_1, color_2) = 
	SQRT (
	      (color_1.r - color_2.r) ^2 + 
              (color_1.g - color_2.g) ^2 + 
              (color_1.b - color_2.b) ^2)
)

-- let's define the new data type
-- so that we can implement the distance functions

CREATE TYPE color AS (r smallint, g smallint, b smallint);

CREATE TABLE colors (
	color color PRIMARY KEY,
	name TEXT
);

CREATE TABLE palettes (

	id INT PRIMARY KEY,
	name TEXT,

	color_1 color,
	color_2 color,
	color_3 color,
	color_4 color,

	likes BIGINT
);

-- load the same data with the new data type
INSERT INTO colors VALUES
('(255, 0  , 0)'  , 'RED'),
('(0  , 0  , 0)'  , 'BLACK'),
('(255, 255, 255)', 'WHITE'),
('(0  , 0  , 255)', 'BLUE');

INSERT INTO palettes VALUES (1, 'OndersPalette','(255, 0  , 0)', '(0  , 0  , 0)', '(255, 255, 255)', '(0  , 0  , 255)', 0);
INSERT INTO palettes VALUES (2, 'BuraksPalette', '(255, 0  , 0)', '(0  , 0  , 0)', '(255, 255, 255)', '(0  , 0  , 255)', 0);

-- now,  simply create a function
CREATE FUNCTION euclidian_distance(rgb_left color, rgb_right color)
 RETURNS float AS $$
	SELECT SQRT( (rgb_left.r - rgb_right.r) ^2 + (rgb_left.g - rgb_right.g) ^2 + (rgb_left.b - rgb_right.b) ^2);
$$
LANGUAGE sql;

-- now, use the function in the query
SELECT 
	* 
FROM 
	palettes 
WHERE 
	euclidian_distance(color_1, '(255,10,0)') < 15 ;

-- we can improve the user experience by introducing new operators
CREATE OPERATOR <-> (
	LEFTARG = color, 
	RIGHTARG = color, 
	PROCEDURE = euclidian_distance,
	COMMUTATOR = '<->'
);

-- the same query with the operator
SELECT 
        * 
FROM 
        palettes 
WHERE 
        color_1 <-> '(255,10,0)' < 15 ;

-- awesome, now we have a pretty nice system
-- are we done?

-- let's see what the performance looks with some more data
--- the life is not that simple, we'll have a lot more data
TRUNCATE palettes, colors CASCADE;

-- lets generate some randome colors
CREATE OR REPLACE FUNCTION generate_all_colors(color_distance int)
RETURNS void AS $$
DECLARE
	r_color_index int :=0;
	g_color_index int :=0;
	b_color_index int :=0;
BEGIN
	
		WHILE r_color_index < 255 LOOP
			WHILE g_color_index < 255 LOOP
				WHILE b_color_index < 255 LOOP
					INSERT INTO colors VALUES (('('||r_color_index::text  || ',' || g_color_index::text || ',' || b_color_index::text ||')')::color,  r_color_index || '_' || g_color_index || '_' || b_color_index);
					b_color_index = b_color_index + color_distance;

				END LOOP;
						b_color_index = 0;

				g_color_index = g_color_index + color_distance;

			END LOOP;
			g_color_index =0;

			r_color_index = r_color_index + color_distance;
		END LOOP;
	return;
END;
$$
LANGUAGE plpgsql;



-- call the function to generate all possible colors
SELECT generate_all_colors(1);

-- find the colors which is pretty close to (255,10,0)
-- oh, now we're safe again, our hard work dropped the
-- query execution time for this query to a lot more
-- reasonable times like 600msecs
SELECT
        count(*)
FROM
        colors
WHERE
        color <-> '(255,10,0)' < 15 ;

