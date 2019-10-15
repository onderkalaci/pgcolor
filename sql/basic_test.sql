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


INSERT INTO colors VALUES
('(255,0,0)', 'RED'),
('(0,0,0)', 'BLACK'),
('(255,255,255)', 'WHITE'),
('(0,0,255)', 'BLUE');

SELECT * FROM colors WHERE color <-> '(255,255,253)' < 5;
