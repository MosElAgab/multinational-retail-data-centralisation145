\c sales_data

-- task 1 
-- orders table
ALTER TABLE "orders_table" 
	ALTER COLUMN date_uuid TYPE UUID USING date_uuid::uuid,
	ALTER COLUMN user_uuid TYPE UUID USING user_uuid::uuid,
	ALTER COLUMN card_number TYPE VARCHAR(40),
	ALTER COLUMN store_code TYPE VARCHAR(40),
	ALTER COLUMN product_code TYPE VARCHAR(40),
	ALTER COLUMN product_quantity TYPE SMALLINT
;


-- task 2
-- dim users
ALTER TABLE dim_users 
	ALTER COLUMN first_name TYPE VARCHAR(255),
	ALTER COLUMN last_name TYPE VARCHAR(255),
	ALTER COLUMN date_of_birth TYPE DATE USING date_of_birth::date,
	ALTER COLUMN country_code TYPE VARCHAR(4),
	ALTER COLUMN user_uuid TYPE UUID USING user_uuid::uuid,
	ALTER COLUMN join_date TYPE DATE USING join_date::date
;


-- task 3
-- dim_store_details

ALTER TABLE dim_store_details
	ALTER COLUMN longitude TYPE FLOAT8 USING CASE 
		WHEN longitude = 'N/A' THEN null ELSE longitude::double precision END,
	ALTER COLUMN locality TYPE VARCHAR(255),
	ALTER COLUMN store_code TYPE VARCHAR(40),
	ALTER COLUMN staff_numbers TYPE SMALLINT USING staff_numbers::smallint,
	ALTER COLUMN opening_date TYPE DATE USING opening_date::date,
	ALTER COLUMN store_type TYPE VARCHAR(255),
	ALTER COLUMN latitude TYPE FLOAT8 USING CASE
		WHEN latitude = 'N/A' THEN null ELSE latitude::double precision END,
	ALTER COLUMN country_code TYPE VARCHAR(4),
	ALTER COLUMN continent TYPE VARCHAR(255)
;


-- task 4
-- dim_products

UPDATE dim_products
SET
	product_price = TRIM(LEADING '£' FROM product_price),
	weight = TRIM(TRAILING 'kg' FROM weight)
;

ALTER TABLE dim_products
	ADD COLUMN weight_class VARCHAR(40) NOT NULL DEFAULT 'N/A'
;

UPDATE dim_products
SET weight_class = CASE
	WHEN CAST(weight AS FLOAT8) < 2 THEN 'Light'
	WHEN CAST(weight AS FLOAT8) >= 2 AND CAST(weight AS FLOAT8) < 40 THEN 'Mid_Sized'
	WHEN CAST(weight AS FLOAT8) >= 40 AND CAST(weight AS FLOAT8) < 140 THEN 'Heavy'
	WHEN CAST(weight AS FLOAT8) >= 140 THEN 'Truck_Required'
	END
;


-- task 5
-- dim_products
ALTER TABLE dim_products
	ADD COLUMN still_avaliable BOOL
;

UPDATE dim_products
SET still_avaliable = CASE
	WHEN removed = 'Still_avaliable' THEN true
	WHEN removed = 'Removed' THEN false
	ELSE null
	END
;

ALTER TABLE dim_products
	DROP COLUMN removed
;

ALTER TABLE "dim_products"
	ALTER COLUMN product_price TYPE FLOAT8 USING product_price::double precision,
	ALTER COLUMN weight TYPE FLOAT8 USING weight::double precision,
	ALTER COLUMN "EAN" TYPE VARCHAR(40),
	ALTER COLUMN product_code TYPE VARCHAR(40),
	ALTER COLUMN date_added TYPE DATE USING date_added::date,
	ALTER COLUMN uuid TYPE UUID USING uuid::uuid,
	ALTER COLUMN weight_class TYPE VARCHAR(40)
	
;


-- task 6 
-- dim_date_times

ALTER TABLE "dim_date_times"
	ALTER COLUMN "month" TYPE VARCHAR(2),
	ALTER COLUMN "year" TYPE VARCHAR(4),
	ALTER COLUMN "day" TYPE VARCHAR(2),
	ALTER COLUMN "time_period" TYPE VARCHAR(40),
	ALTER COLUMN date_uuid TYPE UUID USING date_uuid::uuid
;


-- task 7
-- dim_card_details

ALTER TABLE "dim_card_details"
	ALTER COLUMN "card_number" TYPE VARCHAR(20),
	ALTER COLUMN "expiry_date" TYPE VARCHAR(5),
	ALTER COLUMN "card_provider" TYPE VARCHAR(255),
	ALTER COLUMN date_payment_confirmed TYPE DATE USING date_payment_confirmed::date
;


-- task 8
-- add primary keys to dimension tables
ALTER TABLE "dim_users"
	ADD PRIMARY KEY ("user_uuid")
;
ALTER TABLE "dim_card_details"
	ADD PRIMARY KEY ("card_number")
;
ALTER TABLE "dim_store_details"
	ADD PRIMARY KEY ("store_code")
;
ALTER TABLE "dim_products"
	ADD PRIMARY KEY ("product_code")
;
ALTER TABLE "dim_date_times"
	ADD PRIMARY KEY ("date_uuid")
;


-- task 9
-- add foreign keys contraint to orders tables
ALTER TABLE "orders_table"
	ADD CONSTRAINT fk_dim_users FOREIGN KEY ("user_uuid") REFERENCES dim_users("user_uuid"),
	ADD CONSTRAINT fk_dim_card_details FOREIGN KEY ("card_number") REFERENCES dim_card_details("card_number"),
	ADD CONSTRAINT fk_dim_store_details FOREIGN KEY ("store_code") REFERENCES dim_store_details("store_code"),
	ADD CONSTRAINT fk_dim_products FOREIGN KEY ("product_code") REFERENCES dim_products("product_code"),
	ADD CONSTRAINT fk_dim_date_times FOREIGN KEY ("date_uuid") REFERENCES dim_date_times("date_uuid")
;


-- list all tables
SELECT table_name FROM information_schema.tables
WHERE table_schema = 'public';