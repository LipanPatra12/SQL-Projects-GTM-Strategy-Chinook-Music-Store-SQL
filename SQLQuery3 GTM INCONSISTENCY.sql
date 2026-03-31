-- finding inconsistency in playlist_track table
SELECT 
	playlist_id,
    COUNT(track_id) AS total_tracks_count,
    COUNT(DISTINCT track_id) AS distinct_tracks_count
FROM playlist_track
GROUP BY playlist_id;

-- finding inconsistency in playlist table   
WITH playlist_duplicates AS (
	SELECT 
		name,
		COUNT(DISTINCT playlist_id) AS playlist_count
	FROM playlist
	GROUP BY name
)

SELECT *
FROM playlist 
WHERE name IN (
	SELECT name FROM playlist_duplicates
    WHERE playlist_count > 1
);

-- removing duplicity from playlist table i.e. removing playlist_id 2, 4, 6, 7
DELETE FROM playlist
WHERE playlist_id IN (2, 4, 6, 7); 

SELECT *
FROM artist;

-- finding inconsistency in artist table
SELECT 
	name,
    COUNT(DISTINCT artist_id) AS cnt
FROM artist
GROUP BY name
ORDER BY cnt DESC;

-- finding inconsistency in media_type table
SELECT 
	name,
    COUNT(media_type_id) AS cnt
FROM media_type
GROUP BY name 
ORDER BY cnt DESC;

-- finding inconsistency in genre table
SELECT 
	name,
    COUNT(DISTINCT genre_id) AS cnt
FROM genre
GROUP BY name
ORDER BY cnt DESC;
--finding inconsistency in album table
SELECT 
	title,
    COUNT(DISTINCT album_id) AS cnt
FROM album
GROUP BY title
ORDER BY cnt DESC;


WITH album_duplicates_1 AS (
	SELECT 
		title,
		COUNT(DISTINCT album_id) AS cnt
	FROM album
	GROUP BY title
	
)

SELECT *
FROM album 
WHERE title IN (
	SELECT title FROM album_duplicates_1
    WHERE cnt > 1
);
/*
finding inconsistency in track table 
- check for duplicates
- check for other inconsistency
- handling NULL values
*/
SELECT 
	name,
    COUNT(DISTINCT track_id) AS cnt
FROM track
GROUP BY name
ORDER BY cnt DESC;

WITH track_duplicates_1 AS (
	SELECT 
		name,
		COUNT(DISTINCT track_id) AS cnt
	FROM track
	GROUP BY name
	
),

track_duplicates_2 AS (
	SELECT 
		*,
		COUNT(track_id) OVER(PARTITION BY name) AS cnt
	FROM track
	WHERE name IN (
		SELECT name FROM track_duplicates_1
		WHERE cnt > 1
	)
)

SELECT *
FROM track_duplicates_2 t1
JOIN track_duplicates_2 t2
	ON t1.track_id != t2.track_id 
    AND t1.name = t2.name 
    AND t1.album_id = t2.album_id
    AND t1.genre_id = t2.genre_id
    AND t1.milliseconds = t2.milliseconds;
    
-- manipulating composer column
WITH filtered_track AS (	
    SELECT *
	FROM track
	WHERE track_id NOT IN (	
		SELECT track_id
		FROM track 
		WHERE composer = 'AC/DC' 
   OR composer = 'Mundo Livre S/A' 
   OR composer = 'Luciana Souza/Romero Lubambo'
   OR composer LIKE '%AC/DC%' 
   OR composer LIKE '%Mundo Livre S/A%' 
   OR composer LIKE '%Luciana Souza/Romero Lubambo%'
 ) AND composer LIKE '%/%'
)

UPDATE track
SET composer = REPLACE(composer, '/', ', ')
WHERE track_id IN (
    SELECT track_id
	FROM filtered_track
);

SELECT *
FROM track;

-- replacing all null values in composer column to Unknown
WITH null_composer_track AS (	
    SELECT *
	FROM track 
	WHERE composer IS NULL
)

UPDATE track
SET composer = 'Unknown'
WHERE track_id IN (
	SELECT track_id
	FROM null_composer_track
);

SELECT *
FROM track;

-- finding inconsistency in employee table
SELECT *
FROM employee;


WITH filtered_employee AS (
	SELECT *
	FROM employee
	WHERE phone NOT LIKE '%+%'
)

UPDATE employee
SET phone = CONCAT('+', phone),
	fax = CONCAT('+', fax)
WHERE employee_id IN (
	SELECT employee_id
    FROM filtered_employee
);


/*
finding inconsistency in customer table
- NULL values in company column
- NULL values in state column
- NULL values in phone column
- NULL values in fax column 
- NULL values in postal code column
*/

-- replacing NULL values with Not Applicable (NA)
WITH cust_comp_filter AS (
	SELECT *
	FROM customer
	WHERE company IS NULL
)

UPDATE customer
SET company = 'NA'
WHERE customer_id IN (
	SELECT customer_id
    FROM cust_comp_filter
);

-- replacing NULL value in state with None
WITH state_cust_filter AS (
	SELECT *
    FROM customer
    WHERE state IS NULL
)

UPDATE customer
SET state = 'None'
WHERE customer_id IN (
SELECT customer_id
	FROM state_cust_filter
);

SELECT *
FROM customer;


-- replacing NULL value in phone with None
WITH phone_cust_filter AS (
	SELECT *
    FROM customer
    WHERE phone IS NULL
)

UPDATE customer
SET phone = 'None'
WHERE customer_id IN (
SELECT customer_id
	FROM phone_cust_filter
);

SELECT *
FROM customer;


-- replacing NULL value in fax with None
WITH fax_cust_filter AS (
	SELECT *
    FROM customer
    WHERE fax IS NULL
)

UPDATE customer
SET fax = 'None'
WHERE customer_id IN (
SELECT customer_id
	FROM fax_cust_filter
);

SELECT *
FROM customer;

-- replacing NULL value in postal code with None
WITH post_cust_filter AS (
	SELECT *
    FROM customer
    WHERE postal_code IS NULL
)

UPDATE customer
SET postal_code = 'None'
WHERE customer_id IN (
SELECT customer_id
	FROM post_cust_filter
);

SELECT *
FROM customer;