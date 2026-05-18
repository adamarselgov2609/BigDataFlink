CREATE TABLE kafka_sales_raw (
    id STRING,
    customer_first_name STRING,
    customer_last_name STRING,
    customer_age STRING,
    customer_email STRING,
    customer_country STRING,
    customer_postal_code STRING,
    customer_pet_type STRING,
    customer_pet_name STRING,
    customer_pet_breed STRING,
    seller_first_name STRING,
    seller_last_name STRING,
    seller_email STRING,
    seller_country STRING,
    seller_postal_code STRING,
    product_name STRING,
    product_category STRING,
    product_price STRING,
    product_quantity STRING,
    sale_date STRING,
    sale_customer_id STRING,
    sale_seller_id STRING,
    sale_product_id STRING,
    sale_quantity STRING,
    sale_total_price STRING,
    store_name STRING,
    store_location STRING,
    store_city STRING,
    store_state STRING,
    store_country STRING,
    store_phone STRING,
    store_email STRING,
    pet_category STRING,
    product_weight STRING,
    product_color STRING,
    product_size STRING,
    product_brand STRING,
    product_material STRING,
    product_description STRING,
    product_rating STRING,
    product_reviews STRING,
    product_release_date STRING,
    product_expiry_date STRING,
    supplier_name STRING,
    supplier_contact STRING,
    supplier_email STRING,
    supplier_phone STRING,
    supplier_address STRING,
    supplier_city STRING,
    supplier_country STRING,
    source_file STRING,
    file_index STRING,
    global_sale_id STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'sales_raw',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'flink_sql_group',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true'
);

CREATE TABLE dim_date (
    sale_date DATE,
    day_num INT,
    month_num INT,
    year_num INT,
    PRIMARY KEY (sale_date) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/bigdata_flink',
    'table-name' = 'dwh.dim_date',
    'username' = 'postgres',
    'password' = 'postgres'
);

CREATE TABLE dim_customer (
    customer_id INT,
    customer_first_name STRING,
    customer_last_name STRING,
    customer_age INT,
    customer_email STRING,
    customer_country STRING,
    customer_postal_code STRING,
    PRIMARY KEY (customer_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/bigdata_flink',
    'table-name' = 'dwh.dim_customer',
    'username' = 'postgres',
    'password' = 'postgres'
);

CREATE TABLE dim_customer_pet (
    customer_id INT,
    customer_pet_type STRING,
    customer_pet_name STRING,
    customer_pet_breed STRING,
    pet_category STRING,
    PRIMARY KEY (customer_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/bigdata_flink',
    'table-name' = 'dwh.dim_customer_pet',
    'username' = 'postgres',
    'password' = 'postgres'
);

CREATE TABLE dim_seller (
    seller_id INT,
    seller_first_name STRING,
    seller_last_name STRING,
    seller_email STRING,
    seller_country STRING,
    seller_postal_code STRING,
    PRIMARY KEY (seller_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/bigdata_flink',
    'table-name' = 'dwh.dim_seller',
    'username' = 'postgres',
    'password' = 'postgres'
);

CREATE TABLE dim_product (
    product_id INT,
    product_name STRING,
    product_category STRING,
    product_price DECIMAL(12,2),
    product_quantity INT,
    product_weight DECIMAL(12,2),
    product_color STRING,
    product_size STRING,
    product_brand STRING,
    product_material STRING,
    product_description STRING,
    product_rating DECIMAL(3,1),
    product_reviews INT,
    product_release_date DATE,
    product_expiry_date DATE,
    PRIMARY KEY (product_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/bigdata_flink',
    'table-name' = 'dwh.dim_product',
    'username' = 'postgres',
    'password' = 'postgres'
);

CREATE TABLE dim_store (
    store_name STRING,
    store_location STRING,
    store_city STRING,
    store_state STRING,
    store_country STRING,
    store_phone STRING,
    store_email STRING,
    PRIMARY KEY (store_name) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/bigdata_flink',
    'table-name' = 'dwh.dim_store',
    'username' = 'postgres',
    'password' = 'postgres'
);

CREATE TABLE dim_supplier (
    supplier_name STRING,
    supplier_contact STRING,
    supplier_email STRING,
    supplier_phone STRING,
    supplier_address STRING,
    supplier_city STRING,
    supplier_country STRING,
    PRIMARY KEY (supplier_name) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/bigdata_flink',
    'table-name' = 'dwh.dim_supplier',
    'username' = 'postgres',
    'password' = 'postgres'
);

CREATE TABLE fact_sales (
    sale_id INT,
    sale_date DATE,
    customer_id INT,
    seller_id INT,
    product_id INT,
    store_name STRING,
    supplier_name STRING,
    sale_quantity INT,
    sale_total_price DECIMAL(12,2),
    PRIMARY KEY (sale_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/bigdata_flink',
    'table-name' = 'dwh.fact_sales',
    'username' = 'postgres',
    'password' = 'postgres'
);

EXECUTE STATEMENT SET
BEGIN

INSERT INTO dim_date
SELECT DISTINCT
    TO_DATE(sale_date, 'MM/dd/yyyy') AS sale_date,
    CAST(EXTRACT(DAY FROM TO_DATE(sale_date, 'MM/dd/yyyy')) AS INT) AS day_num,
    CAST(EXTRACT(MONTH FROM TO_DATE(sale_date, 'MM/dd/yyyy')) AS INT) AS month_num,
    CAST(EXTRACT(YEAR FROM TO_DATE(sale_date, 'MM/dd/yyyy')) AS INT) AS year_num
FROM kafka_sales_raw
WHERE sale_date IS NOT NULL
  AND sale_date <> '';

INSERT INTO dim_customer
SELECT DISTINCT
    CAST(sale_customer_id AS INT) AS customer_id,
    customer_first_name,
    customer_last_name,
    CAST(NULLIF(customer_age, '') AS INT) AS customer_age,
    customer_email,
    customer_country,
    customer_postal_code
FROM kafka_sales_raw
WHERE sale_customer_id IS NOT NULL
  AND sale_customer_id <> '';

INSERT INTO dim_customer_pet
SELECT DISTINCT
    CAST(sale_customer_id AS INT) AS customer_id,
    customer_pet_type,
    customer_pet_name,
    customer_pet_breed,
    pet_category
FROM kafka_sales_raw
WHERE sale_customer_id IS NOT NULL
  AND sale_customer_id <> '';

INSERT INTO dim_seller
SELECT DISTINCT
    CAST(sale_seller_id AS INT) AS seller_id,
    seller_first_name,
    seller_last_name,
    seller_email,
    seller_country,
    seller_postal_code
FROM kafka_sales_raw
WHERE sale_seller_id IS NOT NULL
  AND sale_seller_id <> '';

INSERT INTO dim_product
SELECT DISTINCT
    CAST(sale_product_id AS INT) AS product_id,
    product_name,
    product_category,
    CAST(NULLIF(product_price, '') AS DECIMAL(12,2)) AS product_price,
    CAST(NULLIF(product_quantity, '') AS INT) AS product_quantity,
    CAST(NULLIF(product_weight, '') AS DECIMAL(12,2)) AS product_weight,
    product_color,
    product_size,
    product_brand,
    product_material,
    product_description,
    CAST(NULLIF(product_rating, '') AS DECIMAL(3,1)) AS product_rating,
    CAST(NULLIF(product_reviews, '') AS INT) AS product_reviews,
    TO_DATE(NULLIF(product_release_date, ''), 'MM/dd/yyyy') AS product_release_date,
    TO_DATE(NULLIF(product_expiry_date, ''), 'MM/dd/yyyy') AS product_expiry_date
FROM kafka_sales_raw
WHERE sale_product_id IS NOT NULL
  AND sale_product_id <> '';

INSERT INTO dim_store
SELECT DISTINCT
    store_name,
    store_location,
    store_city,
    store_state,
    store_country,
    store_phone,
    store_email
FROM kafka_sales_raw
WHERE store_name IS NOT NULL
  AND store_name <> '';

INSERT INTO dim_supplier
SELECT DISTINCT
    supplier_name,
    supplier_contact,
    supplier_email,
    supplier_phone,
    supplier_address,
    supplier_city,
    supplier_country
FROM kafka_sales_raw
WHERE supplier_name IS NOT NULL
  AND supplier_name <> '';

INSERT INTO fact_sales
SELECT DISTINCT
    CAST(global_sale_id AS INT) AS sale_id,
    TO_DATE(sale_date, 'MM/dd/yyyy') AS sale_date,
    CAST(sale_customer_id AS INT) AS customer_id,
    CAST(sale_seller_id AS INT) AS seller_id,
    CAST(sale_product_id AS INT) AS product_id,
    store_name,
    supplier_name,
    CAST(NULLIF(sale_quantity, '') AS INT) AS sale_quantity,
    CAST(NULLIF(sale_total_price, '') AS DECIMAL(12,2)) AS sale_total_price
FROM kafka_sales_raw
WHERE id IS NOT NULL
  AND id <> ''
  AND sale_date IS NOT NULL
  AND sale_date <> ''
  AND sale_customer_id IS NOT NULL
  AND sale_customer_id <> ''
  AND sale_seller_id IS NOT NULL
  AND sale_seller_id <> ''
  AND sale_product_id IS NOT NULL
  AND sale_product_id <> ''
  AND store_name IS NOT NULL
  AND store_name <> ''
  AND supplier_name IS NOT NULL
  AND supplier_name <> '';

END;
