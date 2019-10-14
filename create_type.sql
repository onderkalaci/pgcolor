


-- the simplest approach for storing the color information
-- is to keep the name of the colors

CREATE TABLE colors (
	color TEXT PRIMARY KEY,
	name TEXT
);


-- a palette simply consists of 4 colors and each color should
-- exists on the colors table (e.g., use foreign keys)
CREATE TABLE palettes (

	id INT PRIMARY KEY,
	name TEXT,

	color_1 TEXT REFERENCES colors (color) ON UPDATE CASCADE,
	color_2 TEXT REFERENCES colors (color) ON UPDATE CASCADE,
	color_3 TEXT REFERENCES colors (color) ON UPDATE CASCADE,
	color_4 TEXT REFERENCES colors (color) ON UPDATE CASCADE,

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


-- oops, we've a problem, the following INSERT will fail
-- the user/application didn't care much about the case sensitivity 
INSERT INTO palettes VALUES (2, 'BuraksPalette', '0xFF0000', '0x000000', '0xFFFFFF', '0x0000FF', 0);

-- you can always use store/query using the lower() functions
-- but that'd end up with verbose queries, and also utilizing
-- indexes/constraints also need to consider capitilization






