/*
FIND THE TOP-SELLING TRACKS AND TOP ARTIST IN THE USA AND IDENTIFY THEIR MOST FAMOUS GENRES
- made a view that has all invoice data of tracks
- made another view that has album and artist
- combining previous two views and using group by country and track to calculate total sales by country combined with genre
- at last using query to find desired results
*/
CREATE VIEW TrackInvoiceDetails AS (
	SELECT 
		il.invoice_line_id,
		il.invoice_id,
		il.track_id,
		il.unit_price,
		il.quantity,
		t.name AS track_name,
		t.album_id,
		t.media_type_id,
		t.genre_id,
		i.customer_id,
		i.invoice_date,
		i.billing_address,
		i.billing_city,
		i.billing_state,
		i.billing_country,
		i.billing_postal_code
	FROM invoice_line il
	JOIN track t
		ON il.track_id = t.track_id
	JOIN invoice i
		ON il.invoice_id = i.invoice_id
);

CREATE VIEW AlbumArtistDetails AS (
	SELECT 
		al.album_id,
		al.title AS album_title,
		al.artist_id,
		ar.name AS artist_name
	FROM album al
	JOIN artist ar
		ON al.artist_id = ar.artist_id
);


CREATE VIEW TrackSalesByCountry AS
SELECT 
    t1.billing_country,
    t1.track_id,
    t1.track_name,
    t2.album_id,
    t2.album_title,
    t2.artist_id,
    t2.artist_name,
    g.genre_id,
    g.name AS genre_name,
    SUM(t1.unit_price * t1.quantity) AS total_sales
FROM TrackInvoiceDetails t1
JOIN AlbumArtistDetails t2
    ON t1.album_id = t2.album_id
JOIN genre g 
    ON g.genre_id = t1.genre_id
GROUP BY 
    t1.billing_country, 
    t1.track_id, 
    t1.track_name,
    t2.album_id,
    t2.album_title,
    t2.artist_id,
    t2.artist_name,
    g.genre_id,
    g.name;

-- TOP 10 best-selling tracks in USA
WITH track_sales_ranking AS (	
    SELECT 
		billing_country,
		track_name,
		artist_name,
		album_title,
		genre_name,
		total_sales,
		RANK() OVER(ORDER BY total_sales DESC) AS rank_by_sales
	FROM TrackSalesByCountry
	WHERE billing_country = 'USA'
)

SELECT *
FROM track_sales_ranking
WHERE rank_by_sales <= 10;

-- Top Artists in TOP 10 tracks by sale in USA
WITH track_sales_ranking AS (	
    SELECT 
		billing_country,
		track_name,
		artist_name,
		album_title,
		genre_name,
		total_sales,
		RANK() OVER(ORDER BY total_sales DESC) AS rank_by_sales
	FROM TrackSalesByCountry
	WHERE billing_country = 'USA'
),

top_10_tracks_by_sale AS (
	SELECT *
	FROM track_sales_ranking
	WHERE rank_by_sales <= 10
)

SELECT
	artist_name,
    COUNT(DISTINCT track_name) AS no_of_track
FROM top_10_tracks_by_sale
GROUP BY artist_name
ORDER BY no_of_track DESC;



-- Top Genre in TOP 10 tracks by sale in USA
WITH track_sales_ranking AS (	
    SELECT 
		billing_country,
		track_name,
		artist_name,
		album_title,
		genre_name,
		total_sales,
		RANK() OVER(ORDER BY total_sales DESC) AS rank_by_sales
	FROM TrackSalesByCountry
	WHERE billing_country = 'USA'
),

top_10_tracks_by_sale AS (
	SELECT *
	FROM track_sales_ranking
	WHERE rank_by_sales <= 10
)

SELECT
	genre_name,
    COUNT(DISTINCT track_name) AS no_of_track
FROM top_10_tracks_by_sale
GROUP BY genre_name
ORDER BY no_of_track DESC;


/*
WHAT IS THE CUSTOMER DEMOGRAPHIC BREAKDOWN (AGE, GENDER, LOCATION) OF CHINOOK'S CUSTOMER BASE?
- Created a view that holds billing details of all customers
- Using queries to find demographics information
*/

CREATE VIEW CustomerBillingDetails AS (
	SELECT 
		c.customer_id,
		c.first_name,
		c.last_name,
		c.company,
		c.city,
		c.state,
		c.country,
		c.email,
		c.support_rep_id,
		t.total_amount
	FROM customer c
	LEFT JOIN (
		SELECT 
			customer_id,
			SUM(unit_price*quantity) AS total_amount
		FROM TrackInvoiceDetails
		GROUP BY customer_id
	) t
		ON c.customer_id = t.customer_id
);

-- customer distribution across country
SELECT 
	country,
    ROUND((COUNT(customer_id)*100 / (SELECT COUNT(customer_id) FROM customer)), 2) AS cust_percentage
FROM CustomerBillingDetails
GROUP BY country
ORDER BY cust_percentage DESC;

-- customer distribution across country, city and state
SELECT 
	country,
    city,
    state,
    ROUND((COUNT(customer_id)*100 / (SELECT COUNT(customer_id) FROM customer)), 2) AS cust_percentage 
FROM CustomerBillingDetails
GROUP BY country, city, state
ORDER BY cust_percentage DESC, country, city;


-- customer distribution by assosiated with company
SELECT 
	(
    CASE WHEN company = 'NA' THEN 'No'
    ELSE 'Yes'
    END
    ) AS associated_with_company,
    ROUND((COUNT(customer_id)*100 / (SELECT COUNT(customer_id) FROM customer)), 2) AS cust_percentage 
FROM CustomerBillingDetails
GROUP BY (
    CASE WHEN company = 'NA' THEN 'No'
    ELSE 'Yes'
    END
    )
ORDER BY cust_percentage DESC;

/*
CALCULATE THE TOTAL REVENUE AND NUMBER OF INVOICES FOR EACH COUNTRY, STATE, AND CITY?
*/
SELECT 
	billing_country AS country,
    billing_state AS state,
    billing_city AS city,
    SUM(unit_price*quantity) AS total_revenue,
    COUNT(DISTINCT invoice_id) AS no_of_invoices
FROM TrackInvoiceDetails
GROUP BY billing_country, billing_state, billing_city
ORDER BY country, total_revenue DESC;

/*
FIND THE TOP 5 CUSTOMERS BY TOTAL REVENUE IN EACH COUNTRY
*/
SELECT *
FROM (
	SELECT 
		country,
		CONCAT(first_name, ' ', last_name) AS customer_name,
		total_amount AS total_revenue,
		RANK() OVER(PARTITION BY country ORDER BY total_amount DESC) AS rank_by_revenue
	FROM CustomerBillingDetails
    
) AS rr
WHERE rank_by_revenue <= 5;

/*
IDENTIFY THE TOP-SELLING TRACK FOR EACH CUSTOMER
*/
WITH track_sales AS (
	SELECT 
		customer_id,
		track_id,
		(
			SELECT DISTINCT invoice_date FROM TrackInvoiceDetails t1
			WHERE t1.customer_id = t.customer_id AND t1.track_id = t.track_id 
		) AS invoice_date,
		SUM(unit_price*quantity) AS total_sales,
		SUM(quantity) AS purchases_made
	FROM TrackInvoiceDetails t
	GROUP BY customer_id, track_id
)
SELECT *
FROM (
	SELECT 
		*,
		DENSE_RANK() OVER(PARTITION BY customer_id ORDER BY total_sales DESC, invoice_date DESC) AS rnk
	FROM track_sales
) ts 
WHERE rnk = 1;

/*
ARE THERE ANY PATTERNS OR TRENDS IN CUSTOMER PURCHASING BEHAVIOUR (E.G., FREQUENCY OF PURCHASES, PREFERRED PAYMENT METHODS, AVERAGE ORDER VALUE)?
- count of track per invoice per customer
- total purchase amount per invoice per customer
- day in between two consecutive purchase
- running total of count of tracks per invoice
- running total of invoice values
- number of different genre purchased
- total tracks purchased
- total orders placed
- total amount spend
- average spent per invoice
- average track count per invoice
- average days between purchase
*/

WITH customer_invoice_details AS (
    SELECT 
        customer_id,
        invoice_id,
        cnt_of_tracks,
        total_invoice_value,
        invoice_date,

  
        ISNULL(
            DATEDIFF(
                DAY,
                LAG(invoice_date) OVER (PARTITION BY customer_id ORDER BY invoice_date),
                invoice_date
            ), 0
        ) AS day_from_last_invoice,

    
        SUM(cnt_of_tracks) OVER (PARTITION BY customer_id ORDER BY invoice_date) 
            AS runn_total_of_cnt_of_track,

        SUM(total_invoice_value) OVER (PARTITION BY customer_id ORDER BY invoice_date) 
            AS runn_total_of_invoice_value

    FROM (
        SELECT 
            t.customer_id,
            t.invoice_id,

            -- Better JOIN instead of subquery
            i.invoice_date,

            COUNT(DISTINCT t.track_id) AS cnt_of_tracks,
            SUM(t.unit_price * t.quantity) AS total_invoice_value

        FROM TrackInvoiceDetails t
        JOIN invoice i 
            ON i.customer_id = t.customer_id 
            AND i.invoice_id = t.invoice_id

        GROUP BY t.customer_id, t.invoice_id, i.invoice_date
    ) AS cit   -- FIX 2: Proper alias
)

SELECT 
    customer_id,

    COUNT(DISTINCT invoice_id) AS cnt_of_order,

    -- Distinct genres per customer
    (
        SELECT COUNT(DISTINCT genre_id) 
        FROM TrackInvoiceDetails t
        WHERE t.customer_id = c.customer_id
    ) AS cnt_of_distinct_genre,

    SUM(cnt_of_tracks) AS total_tracks_purchased,
    SUM(total_invoice_value) AS total_order_value,

    -- FIX 3: ROUND requires 2 args
    ROUND(AVG(cnt_of_tracks), 0) AS avg_track_cnt_per_order,
    ROUND(AVG(total_invoice_value), 2) AS avg_amount_per_order,
    ROUND(AVG(day_from_last_invoice), 0) AS avg_day_between_order

FROM customer_invoice_details c
GROUP BY customer_id;

/*
WHAT IS THE CUSTOMER CHURN RATE?
- yearly churn rate 
- monthly churn rate
*/

-- yearly churn rate
WITH customer_yearly_orders AS (
    SELECT 
        customer_id,
        YEAR(invoice_date) AS years,
        COUNT(DISTINCT track_id) AS orders_placed
    FROM TrackInvoiceDetails
    GROUP BY customer_id, YEAR(invoice_date)
),

customer_per_year AS (
    SELECT 
        years,
        COUNT(customer_id) AS customer_cnt
    FROM customer_yearly_orders
    WHERE orders_placed > 0
    GROUP BY years
),

pre_final AS (
    SELECT 
        years,
        customer_cnt,
        ROUND(
            (
                (LAG(customer_cnt) OVER (ORDER BY years) - customer_cnt) * 100.0
                / LAG(customer_cnt) OVER (ORDER BY years)
            ), 2
        ) AS churn_rate
    FROM customer_per_year
)

SELECT 
    years,
    customer_cnt,
    CASE 
        WHEN churn_rate < 0 THEN 'Gain'
        ELSE CONCAT(churn_rate, '%')
    END AS churn
FROM pre_final
ORDER BY years;


-- monthly churn
WITH customer_yearly_orders AS (
    SELECT 
        customer_id,

       
        FORMAT(invoice_date, 'yyyy-MM') AS month_year,

        COUNT(DISTINCT track_id) AS orders_placed
    FROM TrackInvoiceDetails
    GROUP BY 
        customer_id, 
        FORMAT(invoice_date, 'yyyy-MM')
),

customer_per_year AS (
    SELECT 
        month_year,
        COUNT(customer_id) AS customer_cnt
    FROM customer_yearly_orders
    WHERE orders_placed > 0
    GROUP BY month_year
),

pre_final AS (
    SELECT 
        month_year,
        customer_cnt,

      
        ROUND(
            (
                (LAG(customer_cnt) OVER (ORDER BY month_year) - customer_cnt) * 100.0
                / LAG(customer_cnt) OVER (ORDER BY month_year)
            ), 2
        ) AS churn_rate
    FROM customer_per_year
)

SELECT 
    month_year,
    customer_cnt,

    
    CASE 
        WHEN churn_rate < 0 THEN 'Gain'
        ELSE CONCAT(churn_rate, '%')
    END AS churn

FROM pre_final
ORDER BY month_year;


/*
CALCULATE THE PERCENTAGE OF TOTAL SALES CONTRIBUTED BY EACH GENRE IN THE USA AND IDENTIFY THE BEST-SELLING GENRES AND ARTISTS.
- finding top genre by sales percentage 
- finding top artist that belongs to these genre
*/

SELECT 
	genre_id,
    genre_name,
    ROUND((SUM(total_sales)*100 / (SELECT SUM(total_sales) FROM TrackSalesByCountry WHERE billing_country = 'USA')), 2) AS sales_percentage
FROM TrackSalesByCountry
WHERE billing_country = 'USA'
GROUP BY genre_id, genre_name
ORDER BY sales_percentage DESC;

WITH top_genre AS (
    SELECT 
        genre_id,
        genre_name,
        ROUND(
            (SUM(total_sales) * 100.0 / 
            (SELECT SUM(total_sales) 
             FROM TrackSalesByCountry 
             WHERE billing_country = 'USA')
            ), 2
        ) AS sales_percentage
    FROM TrackSalesByCountry
    WHERE billing_country = 'USA'
    GROUP BY genre_id, genre_name
    HAVING 
        ROUND(
            (SUM(total_sales) * 100.0 / 
            (SELECT SUM(total_sales) 
             FROM TrackSalesByCountry 
             WHERE billing_country = 'USA')
            ), 2
        ) > 10
)

SELECT 
    artist_id,
    artist_name,
    SUM(total_sales) AS total_sales,
    RANK() OVER(ORDER BY SUM(total_sales) DESC) AS rank_by_sales
FROM TrackSalesByCountry
WHERE genre_id IN (
    SELECT genre_id FROM top_genre
)
AND billing_country = 'USA'
GROUP BY artist_id, artist_name
ORDER BY total_sales DESC;

/*
FIND CUSTOMERS WHO HAVE PURCHASED TRACKS FROM AT LEAST 3 DIFFERENT GENRES
*/
WITH filtered_cust AS (
	SELECT 
		customer_id,
		COUNT(DISTINCT genre_id) AS diff_genre_cnt
	FROM TrackInvoiceDetails
	GROUP BY customer_id
	HAVING COUNT(DISTINCT genre_id)> 3
)

SELECT 
	customer_id,
    CONCAT(first_name, ' ', last_name) AS full_name,
    (
    SELECT diff_genre_cnt FROM filtered_cust f
    WHERE f.customer_id = c.customer_id
    ) AS different_genre_cnt
FROM customer c
WHERE customer_id IN (
	SELECT customer_id FROM filtered_cust
);


/*
RANK GENRES BASED ON THEIR SALES PERFORMANCE IN THE USA
*/
SELECT 
	genre_id,
    genre_name,
    SUM(total_sales) AS genre_total_sales,
    DENSE_RANK() OVER(ORDER BY SUM(total_sales) DESC) AS genre_rank_by_sale
FROM TrackSalesByCountry
WHERE billing_country = 'USA'
GROUP BY genre_id, genre_name;


/*
IDENTIFY CUSTOMERS WHO HAVE NOT MADE A PURCHASE IN THE LAST 3 MONTHS
*/
WITH last_date AS (
    SELECT MAX(invoice_date) AS max_date
    FROM TrackInvoiceDetails
)

SELECT 
    c.customer_id,
    CONCAT(c.first_name, ' ', c.last_name) AS full_name
FROM customer c
WHERE c.customer_id NOT IN (
    SELECT DISTINCT t.customer_id
    FROM TrackInvoiceDetails t
    CROSS JOIN last_date ld
    WHERE t.invoice_date >= DATEADD(MONTH, -3, ld.max_date)
);