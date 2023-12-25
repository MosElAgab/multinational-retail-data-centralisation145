\c sales_data

-- Querying the data


-- task 1
\echo '\n*** how many stores does the buisness have and in which country ?'

SELECT country_code AS country, COUNT(store_code) AS total_no_stores
FROM dim_store_details
GROUP BY country_code
ORDER BY total_no_stores DESC
;


-- task 2
\echo '\n*** which locations currently have the most number of stores ?'
SELECT locality, COUNT(store_code) AS total_no_stores
FROM dim_store_details
GROUP BY locality
ORDER BY total_no_stores DESC
LIMIT 10
;


-- task 3
\echo '\n*** which months produced the largest amount of sales ?'

WITH sales_table AS (
SELECT
	orders_table.product_quantity,
	dim_date_times.month,
	dim_products.product_price,
	(dim_products.product_price * orders_table.product_quantity) AS total_payment
FROM orders_table
INNER JOIN dim_date_times ON dim_date_times.date_uuid = orders_table.date_uuid
INNER JOIN dim_products ON dim_products.product_code = orders_table.product_code
)
SELECT
	ROUND(SUM(total_payment)::decimal, 2) AS total_sales,
	"month"
FROM sales_table
GROUP BY "month"
ORDER BY
	total_sales DESC
;


-- task 4
\echo '\n*** how many sales are coming from online ?'
WITH sales_by_location AS (
SELECT 
	orders_table.date_uuid,
	orders_table.product_quantity,
	dim_store_details.store_type,
	CASE
		WHEN dim_store_details.store_type = 'Web Portal' THEN 'Web' ELSE 'Offline' 
	END AS "location"
FROM orders_table
JOIN dim_store_details ON dim_store_details.store_code = orders_table.store_code
)
SELECT
	COUNT(date_uuid) AS numbers_of_sales,
	SUM(product_quantity) AS product_quantity_count,
	"location"
FROM sales_by_location
GROUP BY "location"
ORDER BY numbers_of_sales
;


-- task 5
\echo '\n*** what percentage of sales come through each type of stores ?'
WITH sales_by_location AS (
SELECT 
	orders_table.product_quantity,
	dim_store_details.store_type,
	dim_products.product_price,
	(orders_table.product_quantity * dim_products.product_price) AS total_payment
FROM 
	orders_table
JOIN dim_store_details
	ON dim_store_details.store_code = orders_table.store_code
JOIN dim_products
	ON dim_products.product_code = orders_table.product_code
)
SELECT
	store_type,
	ROUND(SUM(total_payment)::decimal, 2) As total_sales,
	((ROUND(SUM(total_payment)) / (SELECT SUM(total_payment) FROM sales_by_location))* 100 )AS "percentage_total_(%)"
FROM sales_by_location
GROUP BY store_type
ORDER BY total_sales DESC
;


-- task 6
\echo '\n*** which month in each year produced the highest cost of sales ?'
WITH sales_table AS (
SELECT
	orders_table.product_quantity,
	dim_date_times.month,
	dim_date_times.year,
	dim_products.product_price,
	(dim_products.product_price * orders_table.product_quantity) AS total_payment
FROM
	orders_table
INNER JOIN dim_date_times
	ON dim_date_times.date_uuid = orders_table.date_uuid
INNER JOIN dim_products
	ON dim_products.product_code = orders_table.product_code
)
SELECT
	ROUND(SUM(total_payment)::decimal, 2) AS total_sales,
	"year",
	"month"
FROM
	sales_table
GROUP BY
	"year",
	"month"
ORDER BY
	total_sales DESC
LIMIT 10
;


-- task 7
\echo '\n*** what is our staff headcount ?'
SELECT
	SUM(staff_numbers) AS total_staff_numbers,
	country_code
FROM
	dim_store_details
GROUP BY
	country_code
ORDER BY
	total_staff_numbers DESC;


-- task 8
\echo '\n*** which German store type is selling the most ?'
WITH sales_by_location AS (
SELECT 
	orders_table.product_quantity,
	dim_store_details.store_type,
	dim_store_details.country_code,
	dim_products.product_price,
	(orders_table.product_quantity * dim_products.product_price) AS total_payment
FROM 
	orders_table
JOIN dim_store_details
	ON dim_store_details.store_code = orders_table.store_code
JOIN dim_products
	ON dim_products.product_code = orders_table.product_code
WHERE
	dim_store_details.country_code = 'DE'
)
SELECT
	ROUND(SUM(total_payment)::decimal, 2) AS total_sales,
	store_type,
	(SELECT DISTINCT country_code FROM sales_by_location) AS country_code
FROM
	sales_by_location
GROUP BY store_type
ORDER BY
	total_sales
;


-- task 9
\echo '\n*** how quickly the company making sales ?'
