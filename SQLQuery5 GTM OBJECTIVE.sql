/*
RECOMMEND THE THREE ALBUMS FROM THE NEW RECORD LABEL THAT SHOULD BE PRIORITISED FOR ADVERTISING AND PROMOTION IN THE USA BASED ON GENRE SALES ANALYSIS.

APPROACH
- extracting data of last 3 months
- filtering the top 3 genre by sales in USA
- finding albums with high number of tracks in these three genre 
- evaluating sales of these albums
*/
WITH max_date_cte AS (
    SELECT MAX(invoice_date) AS max_date
    FROM TrackInvoiceDetails
),

last_3_months AS (
    SELECT t.*
    FROM TrackInvoiceDetails t
    CROSS JOIN max_date_cte m
    WHERE t.billing_country = 'USA'
      AND t.invoice_date >= DATEADD(MONTH, -3, m.max_date)
),

top_3_genre AS (
    SELECT genre_id
    FROM (
        SELECT 
            genre_id,
            SUM(unit_price * quantity) AS total_sales,
            RANK() OVER (ORDER BY SUM(unit_price * quantity) DESC) AS rank_by_sales
        FROM last_3_months
        GROUP BY genre_id
    ) g
    WHERE rank_by_sales <= 3
),

filtered_data AS (
    SELECT 
        t.album_id,
        t.album_title,
        t.genre_id,
        t.track_id,
        t.total_sales
    FROM TrackSalesByCountry t
    WHERE t.billing_country = 'USA'
      AND t.genre_id IN (SELECT genre_id FROM top_3_genre)
),

album_rankings AS (
    SELECT 
        album_id,
        album_title,
        COUNT(track_id) AS top_genre_track_cnt,
        SUM(total_sales) AS album_total_sales,
        DENSE_RANK() OVER (ORDER BY SUM(total_sales) DESC) AS rank_by_sales
    FROM filtered_data
    GROUP BY album_id, album_title
)

SELECT 
    album_id,
    album_title,
    top_genre_track_cnt,
    album_total_sales,
    rank_by_sales
FROM album_rankings
WHERE rank_by_sales <= 3
ORDER BY album_total_sales DESC;


/*
DETERMINE THE TOP-SELLING GENRES IN COUNTRIES OTHER THAN THE USA AND IDENTIFY ANY COMMONALITIES OR DIFFERENCES.

APPROACH 
- aggregating total sales in USA and in Other Region
- ranking all genre by sales in USA
- ranking all genre by sales in other regions
- comparing them side by side and flagging genre based on rank in USA and in Other Region
*/
WITH genre_wise_sales AS (
    SELECT 
        genre_id,
        genre_name,
        SUM(CASE WHEN billing_country = 'USA' THEN total_sales ELSE 0 END) AS sales_in_USA,
        SUM(CASE WHEN billing_country <> 'USA' THEN total_sales ELSE 0 END) AS sales_in_Others
    FROM TrackSalesByCountry
    GROUP BY genre_id, genre_name
),

ranked_genres AS (
    SELECT 
        genre_id,
        genre_name,
        sales_in_USA,
        sales_in_Others,
        RANK() OVER (ORDER BY sales_in_USA DESC) AS rank_by_sales_USA,
        RANK() OVER (ORDER BY sales_in_Others DESC) AS rank_by_sales_Others
    FROM genre_wise_sales
)

SELECT 
    genre_id,
    genre_name,
    sales_in_USA,
    sales_in_Others,
    rank_by_sales_USA,
    rank_by_sales_Others,
    CASE 
        WHEN rank_by_sales_USA < rank_by_sales_Others THEN 'Higher in USA'
        WHEN rank_by_sales_USA > rank_by_sales_Others THEN 'Lower in USA'
        ELSE 'Same in USA'
    END AS rank_status
FROM ranked_genres
ORDER BY rank_by_sales_USA;


/*
CUSTOMER PURCHASING BEHAVIOR ANALYSIS: 
HOW DO THE PURCHASING HABITS (FREQUENCY, BASKET SIZE, SPENDING AMOUNT) OF LONG-TERM CUSTOMERS DIFFER FROM THOSE OF NEW CUSTOMERS? 
WHAT INSIGHTS CAN THESE PATTERNS PROVIDE ABOUT CUSTOMER LOYALTY AND RETENTION STRATEGIES?

APPROACH
- making a view to customer track purchase data using invoice_line and invoice table
- generating CTE that gives invoice details of all invoices customer wise
- creating customer segmentation and using aggregation to discover customer wise purchase trends
- assuming that customers who have first and last purchase months difference above 36 are long-term customers
- flagging customers as long-term and new customers based on first and recent purchase date
- finding frequency of purchase, basket size and spending amount etc. for each customer
*/

CREATE VIEW CustomerTrackInvoice AS (
	SELECT 
		i1.invoice_line_id,
		i1.invoice_id,
		i1.track_id,
		i1.unit_price,
		i1.quantity,
		i2.customer_id,
		i2.invoice_date,
		i2.billing_city,
		i2.billing_country
	FROM invoice_line i1
	LEFT JOIN invoice i2
		ON i1.invoice_id = i2.invoice_id
);

WITH customer_invoice_data AS (
    SELECT 
        customer_id,
        invoice_id,
        invoice_date,

        DATEDIFF(
            DAY,
            LAG(invoice_date) OVER (PARTITION BY customer_id ORDER BY invoice_date),
            invoice_date
        ) AS days_before_last_purchase,

        COUNT(quantity) AS track_count,
        COUNT(DISTINCT track_id) AS unique_track_count,
        SUM(unit_price * quantity) AS total_purchase_amount

    FROM CustomerTrackInvoice
    GROUP BY customer_id, invoice_id, invoice_date
),

customer_segmentation AS (
    SELECT 
        customer_id,

        CASE
            WHEN DATEDIFF(MONTH, MIN(invoice_date), MAX(invoice_date)) <= 36 THEN 'New'
            ELSE 'Long-Term'
        END AS customer_type,

        DATEDIFF(MONTH, MIN(invoice_date), MAX(invoice_date)) AS since_months, 

        COUNT(invoice_id) AS cnt_of_purchases,

        ROUND(AVG(days_before_last_purchase), 0) AS avg_purchase_frequency_in_days,

        SUM(track_count) AS total_track_count,
        SUM(unique_track_count) AS total_unique_tracks,

        ROUND(AVG(total_purchase_amount), 2) AS avg_purchase_amount,
        SUM(total_purchase_amount) AS total_amount_spent,

        ROUND(1.0 * SUM(track_count) / COUNT(invoice_id), 0) AS avg_basket_size

    FROM customer_invoice_data
    GROUP BY customer_id
)

SELECT 
    customer_type,

    COUNT(customer_id) AS customer_count,

    ROUND(
        COUNT(customer_id) * 100.0 / 
        (SELECT COUNT(DISTINCT customer_id) FROM customer_segmentation),
    2) AS percent_of_customers,

    ROUND(AVG(cnt_of_purchases), 0) AS avg_purchases_made,
    ROUND(AVG(avg_purchase_frequency_in_days), 0) AS purchase_frequency_in_days,
    ROUND(AVG(avg_purchase_amount), 2) AS avg_amount_per_purchase,

    SUM(total_amount_spent) AS total_spents,
    ROUND(AVG(avg_basket_size), 0) AS basket_size

FROM customer_segmentation
GROUP BY customer_type;

/*
PRODUCT AFFINITY ANALYSIS: 
WHICH MUSIC GENRES, ARTISTS, OR ALBUMS ARE FREQUENTLY PURCHASED TOGETHER BY CUSTOMERS? 
HOW CAN THIS INFORMATION GUIDE PRODUCT RECOMMENDATIONS AND CROSS-SELLING INITIATIVES?

APPROACH 
- first joining CustomerTrackInvoice (invoice_line and invoice together) table with album, genre, track and artist table
- using genre_id, album_id and artist_id to group them in a single bundle
- using group by to aggregate quantity which will give how many times combo is bought
- using group by to aggregate distinct customers count who bought the combo 
*/
WITH base_table AS (
	SELECT 
		t.track_id,
		t.name AS track_name,
		g.genre_id,
		g.name AS genre_name,
		a.album_id,
		a.title AS album_title,
		r.artist_id,
		r.name AS artist_name,
		c.quantity,
		c.customer_id
	FROM CustomerTrackInvoice c
	JOIN track t
		ON c.track_id = t.track_id
	JOIN album a 
		ON t.album_id = a.album_id
	JOIN artist r 
		ON r.artist_id = a.artist_id
	JOIN genre g
		ON t.genre_id = g.genre_id
)

SELECT 
	genre_id,
	genre_name,
	album_id,
	album_title,
	artist_id,
	artist_name,
	COALESCE(SUM(quantity), 0) AS total_times_combo_purchased,
	COALESCE(COUNT(DISTINCT customer_id), 0) AS combo_bought_by_customers
FROM base_table
GROUP BY genre_id, genre_name, album_id, album_title, artist_id, artist_name
ORDER BY total_times_combo_purchased DESC, combo_bought_by_customers DESC;


/*
REGIONAL MARKET ANALYSIS: 
DO CUSTOMER PURCHASING BEHAVIORS AND CHURN RATES VARY ACROSS DIFFERENT GEOGRAPHIC REGIONS OR STORE LOCATIONS? 
HOW MIGHT THESE CORRELATE WITH LOCAL DEMOGRAPHIC OR ECONOMIC FACTORS?

APPROACH 
- joining customer geographical data with purchase data
- ranking country by total_sales in lifetime then selecting Top 10 countries
- ranking cities in these countries based on total_sales contribution 
- filtering the cities as entry point in these country based on ranking (i.e. rank 1 for top priority) 
- generating year wise sales in these selected cities to see customer or sales change over the year
- comparing starting and recent year sales to estimate if market is growing or not
*/
CREATE VIEW EntryCityPerCountry AS
WITH customer_transactions AS (
	SELECT 
		t.invoice_id,
		t.track_id,
		t.unit_price,
		t.quantity,
		t.track_name,
		t.album_id,
		t.genre_id,
		t.invoice_date,
		t.customer_id,
		c.city,
		c.state,
		c.country,
		c.support_rep_id
	FROM TrackInvoiceDetails t
	JOIN customer c
		ON c.customer_id = t.customer_id
),

country_wise_rank AS (
	SELECT 
		DENSE_RANK() OVER(ORDER BY SUM(unit_price*quantity) DESC) AS country_rank,
		country,
		COUNT(DISTINCT customer_id) AS cnt_of_customer,
		COUNT(DISTINCT invoice_id) AS cnt_of_invoice,
		COUNT(DISTINCT track_id) AS cnt_of_tracks,
		SUM(unit_price*quantity) AS sales_generated,
		ROUND(
			SUM(unit_price*quantity)*100.0 / (
				SELECT SUM(unit_price*quantity) FROM customer_transactions
			), 2
		) AS percent_of_total_country_sale
	FROM customer_transactions
	GROUP BY country
),

entry_city_ranking AS (
	SELECT 
		country,
		city,
		COUNT(DISTINCT customer_id) AS cnt_of_customer,
		COUNT(DISTINCT invoice_id) AS cnt_of_invoice,
		COUNT(DISTINCT track_id) AS cnt_of_tracks,
		SUM(unit_price*quantity) AS sales_generated,
		ROUND(
			SUM(unit_price*quantity)*100.0 / (
				SELECT SUM(unit_price*quantity) 
				FROM customer_transactions c1
				WHERE c1.country = c.country
			), 2
		) AS percent_of_total_country_sale,
		RANK() OVER(PARTITION BY country ORDER BY SUM(unit_price*quantity) DESC) AS priority_of_entry
	FROM customer_transactions c
	WHERE country IN (
		SELECT country FROM country_wise_rank WHERE country_rank <= 10
	)
	GROUP BY country, city
)

SELECT *
FROM entry_city_ranking
WHERE priority_of_entry = 1;


/*
CUSTOMER RISK PROFILING: 
Based on customer profiles (age, gender, location, purchase history), which customer segments are more likely to churn or pose a higher risk of reduced spending? 
What factors contribute to this risk?

APPROACH 
- first finding customer that have high risk and storing in view
- using count of tracks and purchase amount of individual customers each year
- deriving change from previous year using lag function
- counting the no. of years which have a decrease (negative change) in tracks and amount 
- finding if the decrease is happening in 2020 or not using a column
- using both the above mention condition to find high risk customers
- then analysing geographical and purchase history of these customers
*/
CREATE VIEW HighRiskCustomer AS
WITH cust_purchase_this_year AS (
	SELECT 
		customer_id,
		YEAR(invoice_date) AS years,
		COUNT(quantity) AS this_year_track_cnt,
		SUM(unit_price * quantity) AS this_year_purchase_amt
	FROM TrackInvoiceDetails
	GROUP BY customer_id, YEAR(invoice_date)
),

cust_purchase_change AS (
	SELECT 
		customer_id,
		years,
		this_year_track_cnt,
		this_year_purchase_amt,

		COALESCE(
			ROUND(
				(this_year_track_cnt - LAG(this_year_track_cnt) OVER(PARTITION BY customer_id ORDER BY years)) * 100.0
				/ NULLIF(LAG(this_year_track_cnt) OVER(PARTITION BY customer_id ORDER BY years), 0)
			, 2)
		, 0) AS track_change,

		COALESCE(
			ROUND(
				(this_year_purchase_amt - LAG(this_year_purchase_amt) OVER(PARTITION BY customer_id ORDER BY years)) * 100.0
				/ NULLIF(LAG(this_year_purchase_amt) OVER(PARTITION BY customer_id ORDER BY years), 0)
			, 2)
		, 0) AS purchase_change

	FROM cust_purchase_this_year
),

cust_risk_data AS (
	SELECT 
		customer_id,

		SUM(CASE WHEN track_change < 0 THEN 1 ELSE 0 END) AS decrease_count,

		SUM(
			CASE 
				WHEN years = 2020 AND track_change > 0 THEN 1 
				ELSE 0 
			END
		) AS increase_in_2020

	FROM cust_purchase_change
	GROUP BY customer_id
)

SELECT *
FROM customer
WHERE customer_id IN (
	SELECT customer_id
	FROM cust_risk_data 
	WHERE decrease_count >= 2 
		AND increase_in_2020 = 0
);

-- geographical 
SELECT 
	country,
    (
		SELECT COUNT(DISTINCT customer_id) 
		FROM customer c
        WHERE c.country = h.country
    ) AS cnt_of_total_customer,

    COUNT(DISTINCT customer_id) AS cnt_of_risky_customer,

    ROUND(
        COUNT(DISTINCT customer_id) * 100.0 / (
            SELECT COUNT(DISTINCT customer_id) 
            FROM customer c
            WHERE c.country = h.country
        ),
    2) AS percent_of_risky_customer

FROM HighRiskCustomer h
GROUP BY country
ORDER BY percent_of_risky_customer DESC, cnt_of_risky_customer DESC;


-- finding purchase history of high risk customers
-- genre id
SELECT 
	t.genre_id,
	g.name AS genre_name,

	COUNT(DISTINCT t.customer_id) AS cnt_of_risky_customer,
	total.cnt_of_customer,

	ROUND(
		COUNT(DISTINCT t.customer_id) * 100.0 / total.cnt_of_customer,
	2) AS percent_of_risky_customer

FROM TrackInvoiceDetails t

JOIN genre g
	ON g.genre_id = t.genre_id

JOIN (
	SELECT 
		genre_id,
		COUNT(DISTINCT customer_id) AS cnt_of_customer
	FROM TrackInvoiceDetails
	GROUP BY genre_id
) total
	ON t.genre_id = total.genre_id

WHERE t.customer_id IN (
	SELECT customer_id FROM HighRiskCustomer
)

GROUP BY 
	t.genre_id,
	g.name,
	total.cnt_of_customer

ORDER BY percent_of_risky_customer DESC, cnt_of_risky_customer DESC;


-- album 
SELECT 
	t.album_id,
	a.title AS album_title,

	COUNT(DISTINCT t.customer_id) AS cnt_of_risky_customer,
	total.cnt_of_customer,

	ROUND(
		COUNT(DISTINCT t.customer_id) * 100.0 / total.cnt_of_customer,
	2) AS percent_of_risky_customer

FROM TrackInvoiceDetails t

JOIN album a
	ON a.album_id = t.album_id

JOIN (
	SELECT 
		album_id,
		COUNT(DISTINCT customer_id) AS cnt_of_customer
	FROM TrackInvoiceDetails
	GROUP BY album_id
) total
	ON t.album_id = total.album_id

WHERE t.customer_id IN (
	SELECT customer_id FROM HighRiskCustomer
)

GROUP BY 
	t.album_id,
	a.title,
	total.cnt_of_customer

ORDER BY percent_of_risky_customer DESC, cnt_of_risky_customer DESC;


-- track count and purchase amount
SELECT 
	t.customer_id,

	CASE 
		WHEN DATEDIFF(MONTH, MIN(t.invoice_date), MAX(t.invoice_date)) <= 36 
			THEN 'New'
		ELSE 'Long-Term'
	END AS customer_type,

	COUNT(DISTINCT t.invoice_id) AS cnt_of_purchases_made,
	SUM(t.quantity) AS cnt_of_tracks,
	SUM(t.unit_price * t.quantity) AS tot_purchase_amount

FROM TrackInvoiceDetails t

WHERE t.customer_id IN (
	SELECT customer_id FROM HighRiskCustomer
)

GROUP BY t.customer_id;


/*
CUSTOMER LIFETIME VALUE MODELLING: 
How can you leverage customer data (tenure, purchase history, engagement) to predict the lifetime value of different customer segments? 
This could inform targeted marketing and loyalty program strategies. Can you observe any common characteristics or purchase patterns among customers who have stopped purchasing?

- using TrackInvoiceDetails view to find major parameters of Customer Lifecycle 
- ranking album, genre and artist by sales and count of tracks
- using subquery to take table with rank 1 to find top genre, top artist and top album
- finding the amount it contributes to overall lifetime value
- deriving the final table as given below
*/
WITH customer_lifetime_analysis AS (
    SELECT 
        t.customer_id,
        c.first_name + ' ' + c.last_name AS customer_name,
        c.city,
        c.country,
        MIN(invoice_date) AS first_purchase_date,
        MAX(invoice_date) AS last_purchase_date,
        DATEDIFF(MONTH, MIN(invoice_date), MAX(invoice_date)) AS tenure_in_month,
        CASE WHEN YEAR(MAX(invoice_date)) = 2020 THEN 'No' ELSE 'Yes' END AS churn_in_2020,
        CASE WHEN DATEDIFF(MONTH, MIN(invoice_date), MAX(invoice_date)) <= 36 THEN 'New' ELSE 'Long-Term' END AS customer_type,
        SUM(unit_price*quantity) AS lifetime_value,
        COUNT(DISTINCT invoice_id) AS lifetime_purchase_cnt,
        SUM(quantity) AS lifetime_track_cnt,
        CASE 
            WHEN COUNT(DISTINCT invoice_id) > 0 
            THEN ROUND(CAST(DATEDIFF(DAY, MIN(invoice_date), MAX(invoice_date)) AS FLOAT) / COUNT(DISTINCT invoice_id),0)
            ELSE 0
        END AS frequency_of_purchase
    FROM TrackInvoiceDetails t
    JOIN customer c ON t.customer_id = c.customer_id
    GROUP BY t.customer_id, c.first_name, c.last_name, c.city, c.country
),

customer_album_analysis AS (
    SELECT 
        t.customer_id,
        t.album_id,
        a.title AS album_title,
        a.artist_id,
        art.name AS artist_name,
        SUM(unit_price*quantity) AS tot_album_purchase_amt,
        COUNT(track_id) AS track_cnt_in_album,
        ROW_NUMBER() OVER(PARTITION BY t.customer_id ORDER BY SUM(unit_price*quantity) DESC, COUNT(track_id) DESC, a.title) AS album_rnk_by_value
    FROM TrackInvoiceDetails t
    JOIN album a ON t.album_id = a.album_id
    JOIN artist art ON a.artist_id = art.artist_id
    GROUP BY t.customer_id, t.album_id, a.title, a.artist_id, art.name
),

customer_genre_analysis AS (
    SELECT 
        t.customer_id,
        t.genre_id,
        g.name AS genre_name,
        SUM(unit_price*quantity) AS tot_genre_purchase_amt,
        COUNT(track_id) AS track_cnt_in_genre,
        ROW_NUMBER() OVER(PARTITION BY t.customer_id ORDER BY SUM(unit_price*quantity) DESC, COUNT(track_id) DESC) AS genre_rnk_by_value
    FROM TrackInvoiceDetails t
    JOIN genre g ON t.genre_id = g.genre_id
    GROUP BY t.customer_id, t.genre_id, g.name
)

SELECT 
    la.customer_id, 
    la.customer_name, 
    la.city, 
    la.country, 
    CASE WHEN la.customer_id IN (SELECT customer_id FROM HighRiskCustomer) THEN 'Yes' ELSE 'No' END AS is_risky_customer,
    la.first_purchase_date, 
    la.last_purchase_date, 
    la.tenure_in_month, 
    CASE WHEN DATEADD(MONTH, -6, (SELECT MAX(invoice_date) FROM TrackInvoiceDetails)) < la.last_purchase_date THEN 'Yes' ELSE 'No' END AS purchase_in_last_6_month,
    la.churn_in_2020, 
    la.customer_type, 
    la.lifetime_value, 
    la.lifetime_purchase_cnt, 
    la.lifetime_track_cnt, 
    la.frequency_of_purchase, 
    aa.album_title AS fav_album_of_cust,
    aa.track_cnt_in_album,
    aa.artist_name AS fav_artist_of_cust, 
    aa.tot_album_purchase_amt AS tot_purchase_amt,
    ROUND(CAST(aa.tot_album_purchase_amt AS FLOAT) / NULLIF(la.lifetime_value,0), 2) AS percent_of_lifetime_value_a,
    ga.genre_name AS fav_genre_of_cust, 
    ga.track_cnt_in_genre,
    ga.tot_genre_purchase_amt, 
    ROUND(CAST(ga.tot_genre_purchase_amt AS FLOAT) / NULLIF(la.lifetime_value,0), 2) AS percent_of_lifetime_value_g
FROM customer_lifetime_analysis la
JOIN (
    SELECT *
    FROM customer_album_analysis
    WHERE album_rnk_by_value = 1
) aa ON aa.customer_id = la.customer_id
JOIN (
    SELECT *
    FROM customer_genre_analysis
    WHERE genre_rnk_by_value = 1
) ga ON ga.customer_id = la.customer_id;
    
    
/*
HOW CAN YOU ALTER THE "ALBUMS" TABLE TO ADD A NEW COLUMN NAMED "RELEASEYEAR" OF TYPE INTEGER TO STORE THE RELEASE YEAR OF EACH ALBUM?
*/
-- Check table structure in SQL Server
EXEC sp_help 'album';

-- Add a new column 'release_year' of type INT
ALTER TABLE album
ADD release_year INT;

-- Verify column added
EXEC sp_help 'album';

/*
CHINOOK IS INTERESTED IN UNDERSTANDING THE PURCHASING BEHAVIOR OF CUSTOMERS BASED ON THEIR GEOGRAPHICAL LOCATION. 
THEY WANT TO KNOW THE AVERAGE TOTAL AMOUNT SPENT BY CUSTOMERS FROM EACH COUNTRY, ALONG WITH THE NUMBER OF CUSTOMERS AND THE AVERAGE NUMBER OF TRACKS PURCHASED PER CUSTOMER. 
WRITE AN SQL QUERY TO PROVIDE THIS INFORMATION.
*/
WITH base_table AS (
	SELECT 
		c.country,
		SUM(l.unit_price*l.quantity) AS tot_amount_spent,
		COUNT(DISTINCT c.customer_id) AS cnt_of_customers,
		SUM(l.quantity) AS tot_tracks_purchased
	FROM invoice_line l
	JOIN invoice i
		ON i.invoice_id = l.invoice_id
	JOIN customer c
		ON c.customer_id = i.customer_id
	GROUP BY c.country
)

SELECT 
	country,
    cnt_of_customers,
    ROUND((tot_amount_spent / cnt_of_customers), 2) AS avg_tot_amount_spent_per_cust,
    ROUND((tot_tracks_purchased / cnt_of_customers),2) AS avg_cnt_of_tracks_purchased_per_cust
FROM base_table
ORDER BY cnt_of_customers DESC;