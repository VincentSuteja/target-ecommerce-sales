--Create table customers
CREATE TABLE IF NOT EXISTS public."customers"(
	customer_id text COLLATE pg_catalog."default",
	customer_unique_id text COLLATE pg_catalog."default",
	customer_zip_code_prefix integer,
	customer_city text COLLATE pg_catalog."default",
	customer_state text COLLATE pg_catalog."default"
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public."customers" OWNER to postgres;

COPY public."customers" FROM 'E:/DataCamp/Projects/2_E-Commerce Target Sales/customers.csv' DELIMITER ',' CSV HEADER;

--Create table geolocation
CREATE TABLE IF NOT EXISTS public."geolocation"(
	geolocation_zip_code_prefix integer,
	geolocation_lat text COLLATE pg_catalog."default",
	geolocation_lng text COLLATE pg_catalog."default",
	geolocation_city text COLLATE pg_catalog."default",
	geolocation_state text COLLATE pg_catalog."default"
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public."geolocation" OWNER TO postgres;

COPY public."geolocation" FROM 'E:/DataCamp/Projects/2_E-Commerce Target Sales/geolocation.csv' DELIMITER ',' CSV HEADER;

--Create table order_items
CREATE TABLE IF NOT EXISTS public."order_items"(
	order_id text COLLATE pg_catalog."default",
	order_item_id integer,
	product_id text COLLATE pg_catalog."default",
	seller_id text COLLATE pg_catalog."default",
	shipping_limit_date timestamp,
	price numeric,
	freight_value numeric
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public."order_items" OWNER TO postgres;

COPY public."order_items" FROM 'E:/DataCamp/Projects/2_E-Commerce Target Sales/order_items.csv' DELIMITER ',' CSV HEADER;

--Create table orders
CREATE TABLE IF NOT EXISTS public."orders"(
	order_id text COLLATE pg_catalog."default",
	customer_id text COLLATE pg_catalog."default",
	order_status text COLLATE pg_catalog."default",
	order_purchase_timestamp timestamp,
	order_approved_at timestamp,
	order_delivered_carrier_date timestamp,
	order_delivered_customer_date timestamp,
	order_estimated_delivery_date timestamp
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public."orders" OWNER TO postgres;

COPY public."orders" FROM 'E:/DataCamp/Projects/2_E-Commerce Target Sales/orders.csv' DELIMITER ',' CSV HEADER;

--Create table payments
CREATE TABLE IF NOT EXISTS public."payments"(
	order_id text COLLATE pg_catalog."default",
	payment_sequential integer,
	payment_type text COLLATE pg_catalog."default",
	payment_installments integer,
	payment_value numeric
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public."payments" OWNER TO postgres;

COPY public."payments" FROM 'E:/DataCamp/Projects/2_E-Commerce Target Sales/payments.csv' DELIMITER ',' CSV HEADER;

--Create table products
CREATE TABLE IF NOT EXISTS public."products"(
	product_id text COLLATE pg_catalog."default",
	product_category text COLLATE pg_catalog."default",
	product_name_length integer,
	product_description_length integer,
	product_photos_qty integer,
	product_weight_g numeric,
	product_length_cm numeric,
	product_height_cm numeric,
	product_width_cm numeric
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public."products" OWNER TO postgres;

COPY public."products" FROM 'E:/DataCamp/Projects/2_E-Commerce Target Sales/products.csv' DELIMITER ',' CSV HEADER;

--Create table sellers
CREATE TABLE IF NOT EXISTS public."sellers"(
	seller_id text COLLATE pg_catalog."default",
	seller_zip_code_prefix integer,
	seller_city text COLLATE pg_catalog."default",
	seller_state text COLLATE pg_catalog."default"
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public."sellers" OWNER TO postgres;

COPY public."sellers" FROM 'E:/DataCamp/Projects/2_E-Commerce Target Sales/sellers.csv' DELIMITER ',' CSV HEADER;

-- #1 Percentage of completed orders all time
SELECT ROUND(COUNT(*)/(SELECT COUNT(*)::numeric FROM orders)*100.00,2) AS complete_order_percentage
FROM orders
WHERE order_status = 'delivered';

-- #2 Delivery satisfactory all time
WITH cte AS(
	SELECT
	order_id,
	CASE 
		WHEN (order_delivered_customer_date - order_estimated_delivery_date) <= INTERVAL '0 days' THEN 'On Time'
		ELSE 'Late'
		END AS customer_satisfaction
FROM orders
WHERE order_delivered_customer_date IS NOT NULL
)
SELECT
	customer_satisfaction,
	COUNT(*),
	ROUND(COUNT(*)/(SELECT COUNT(*)::numeric FROM orders WHERE order_delivered_customer_date IS NOT NULL)*100,2) AS percentage
FROM cte
GROUP BY customer_satisfaction
ORDER BY customer_satisfaction;

-- #3 Top 5 product category all time
WITH cte AS(
	SELECT
		CASE WHEN rank <= 5 THEN product_category ELSE 'Other' END AS product_category,
		count
	FROM(
		SELECT
			product_category,
			count,
			RANK()OVER(ORDER BY count DESC) AS rank
		FROM(
			SELECT
				INITCAP(COALESCE(p.product_category, 'Other')) AS product_category,
				COUNT(*) AS count
			FROM order_items AS ot
			JOIN products AS p
			ON ot.product_id = p.product_id
			GROUP BY p.product_category
			ORDER BY count DESC
			) AS subquery
		ORDER BY rank
		)
	)
SELECT
	product_category,
	SUM(count) AS order_amount,
	ROUND(SUM(count)/(SELECT COUNT(*) FROM order_items)*100.00, 2) AS percentage
FROM cte
GROUP BY product_category
ORDER BY order_amount DESC;

-- #4 Monthly sales continuous
WITH cte AS(
SELECT
	a.price,
	TO_CHAR(b.order_purchase_timestamp, 'YYYY-MM') AS year
FROM order_items AS a
LEFT JOIN orders AS b
ON a.order_id = b.order_id
WHERE b.order_purchase_timestamp > '2017-01-01'
AND b.order_status = 'delivered'
)
SELECT
	COUNT(*) AS total_orders,
	SUM(price) AS sum_price,
	year
FROM cte
GROUP BY year
ORDER BY sum_price;

-- #5 Days and Hours with order amount
SELECT
	TO_CHAR(order_purchase_timestamp, 'FMDay') AS day,
	EXTRACT(hour FROM order_purchase_timestamp) AS hour,
	COUNT(*) AS order_amount
FROM orders
WHERE order_status = 'delivered'
GROUP BY day, hour
ORDER BY order_amount DESC
LIMIT 5;

-- #6 Order amount by day discrete
SELECT
	TO_CHAR(order_purchase_timestamp, 'FMDay') AS day,
	COUNT(*) AS order_amount,
	ROUND(COUNT(*)/(SELECT COUNT(*)::numeric FROM orders WHERE order_status = 'delivered')*100.00,2) AS percentage
FROM orders
WHERE order_status = 'delivered'
GROUP BY day
ORDER BY order_amount DESC;

-- #7 Order amount by payment method
SELECT
	DISTINCT payment_type,
	COUNT(*) AS amount
FROM payments
GROUP BY payment_type
ORDER BY amount DESC;

SELECT
	payment_type,
	payment_installments,
	COUNT(*) AS amount
FROM payments
WHERE payment_type = 'credit_card'
GROUP BY payment_type, payment_installments
ORDER BY amount DESC;

-- #8 Sales contribution by State
SELECT
	c.customer_state,
	SUM(oi.price) AS sales,
	ROUND(SUM(oi.price)/(SELECT SUM(price)::numeric FROM order_items)*100.00,2) AS sales_percentage
FROM orders AS o
JOIN order_items AS oi
ON o.order_id = oi.order_id
JOIN customers AS c
ON o.customer_id = c.customer_id
WHERE o.order_status = 'delivered'
GROUP BY c.customer_state
ORDER BY sales DESC;

--Tableau
--Order locations
SELECT
	c.customer_state AS customer_state,
	gc.avg_lat AS customer_latitude, 
	gc.avg_lng AS customer_longitude,
	INITCAP(COALESCE(pr.product_category,'Unknown')) AS product_category,
	o.order_status AS order_status,
	gs.avg_lat AS seller_latitude, 
	gs.avg_lng AS seller_longitude,
	py.payment_value AS sales,
	py.payment_type AS payment_type,
	o.order_purchase_timestamp AS order_purchase_timestamp,
	o.order_delivered_customer_date AS order_delivered_customer_date
FROM orders AS o
JOIN customers AS c
ON o.customer_id = c.customer_id
JOIN 
	(SELECT
		geolocation_state,
		AVG(geolocation_lat::numeric)::DOUBLE PRECISION AS avg_lat,
		AVG(geolocation_lng::numeric)::DOUBLE PRECISION AS avg_lng
	FROM geolocation
	GROUP BY geolocation_state) AS gc
ON c.customer_state = gc.geolocation_state
JOIN order_items AS oi
ON o.order_id = oi.order_id
JOIN products AS pr
ON pr.product_id = oi.product_id
JOIN payments AS py
ON o.order_id = py.order_id
JOIN sellers AS s
ON oi.seller_id = s.seller_id
JOIN
	(SELECT
		geolocation_state,
		AVG(geolocation_lat::numeric)::DOUBLE PRECISION AS avg_lat,
		AVG(geolocation_lng::numeric)::DOUBLE PRECISION AS avg_lng
	FROM geolocation
	GROUP BY geolocation_state) AS gs
ON s.seller_state = gs.geolocation_state
GROUP BY customer_state, 
	customer_latitude, 
	customer_longitude,
	product_category, 
	order_status, 
	seller_latitude, 
	seller_longitude,
	sales,
	payment_type,
	order_purchase_timestamp,
	order_delivered_customer_date;