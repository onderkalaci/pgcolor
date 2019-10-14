
-- so, let's solve the immediate problem of the case-insensitive
-- queries and index/constraint utilizations
-- luckily Postgresql contrib packagaes have an extension
-- which solves the issue for us: 
-- CITEXT: https://www.postgresql.org/docs/12/citext.html
CREATE EXTENSION citext;


-- the simplest approach for storing the color information
-- is to keep the name of the colors

CREATE TABLE colors (
	color CITEXT PRIMARY KEY,
	name TEXT
);


-- a palette simply consists of 4 colors and each color should
-- exists on the colors table (e.g., use foreign keys)
CREATE TABLE palettes (

	id INT PRIMARY KEY,
	name TEXT,

	color_1 CITEXT REFERENCES colors (color) ON UPDATE CASCADE,
	color_2 CITEXT REFERENCES colors (color) ON UPDATE CASCADE,
	color_3 CITEXT REFERENCES colors (color) ON UPDATE CASCADE,
	color_4 CITEXT REFERENCES colors (color) ON UPDATE CASCADE,

	likes BIGINT
);


-- start ingesting some basic colors in
-- hex format
INSERT INTO colors VALUES
	('0xff0000', 'RED'),
	('0x000000', 'BLACK'),
	('0xffffff', 'WHITE'),
	('0x0000ff', 'BLUE');

-- now, create the first pallette
INSERT INTO palettes VALUES (1, 'OndersPalette', '0xff0000', '0x000000', '0xffffff', '0x0000ff', 0);


-- the user/application didn't care much about the case sensitivity 
-- but, that's already handled for us
INSERT INTO palettes VALUES (2, 'BuraksPalette', '0xFF0000', '0x000000', '0xFFFFFF', '0x0000FF', 0);

-- with storing the data as a text of the hex representation of the colors
-- seems like an incomplete approach because we're losing the color information
-- on every operation, we'd need to have verbose and error prone queries

-- to give you some insights of what it'd look like
-- while getting the rgb values
SELECT
    name,
	('x' || substr(color,3,2))::bit(8)::int as r, 
	('x' || substr(color,5,2))::bit(8)::int as g, 
	('x' || substr(color,7,2))::bit(8)::int as b 
FROM 
	colors; 

-- so, before things get very complicated, let's swith to
-- a more generic/easy to use represantation for colors

