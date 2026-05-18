CREATE SCHEMA IF NOT EXISTS dwh;

CREATE TABLE IF NOT EXISTS dwh.dim_date (
    sale_date DATE PRIMARY KEY,
    day_num INT NOT NULL,
    month_num INT NOT NULL,
    year_num INT NOT NULL
);

CREATE TABLE IF NOT EXISTS dwh.dim_customer (
    customer_id INT PRIMARY KEY,
    customer_first_name TEXT,
    customer_last_name TEXT,
    customer_age INT,
    customer_email TEXT,
    customer_country TEXT,
    customer_postal_code TEXT
);

CREATE TABLE IF NOT EXISTS dwh.dim_customer_pet (
    customer_id INT PRIMARY KEY REFERENCES dwh.dim_customer(customer_id),
    customer_pet_type TEXT,
    customer_pet_name TEXT,
    customer_pet_breed TEXT,
    pet_category TEXT
);

CREATE TABLE IF NOT EXISTS dwh.dim_seller (
    seller_id INT PRIMARY KEY,
    seller_first_name TEXT,
    seller_last_name TEXT,
    seller_email TEXT,
    seller_country TEXT,
    seller_postal_code TEXT
);

CREATE TABLE IF NOT EXISTS dwh.dim_product (
    product_id INT PRIMARY KEY,
    product_name TEXT,
    product_category TEXT,
    product_price NUMERIC(12,2),
    product_quantity INT,
    product_weight NUMERIC(12,2),
    product_color TEXT,
    product_size TEXT,
    product_brand TEXT,
    product_material TEXT,
    product_description TEXT,
    product_rating NUMERIC(3,1),
    product_reviews INT,
    product_release_date DATE,
    product_expiry_date DATE
);

CREATE TABLE IF NOT EXISTS dwh.dim_store (
    store_name TEXT PRIMARY KEY,
    store_location TEXT,
    store_city TEXT,
    store_state TEXT,
    store_country TEXT,
    store_phone TEXT,
    store_email TEXT
);

CREATE TABLE IF NOT EXISTS dwh.dim_supplier (
    supplier_name TEXT PRIMARY KEY,
    supplier_contact TEXT,
    supplier_email TEXT,
    supplier_phone TEXT,
    supplier_address TEXT,
    supplier_city TEXT,
    supplier_country TEXT
);

CREATE TABLE IF NOT EXISTS dwh.fact_sales (
    sale_id INT PRIMARY KEY,
    sale_date DATE NOT NULL REFERENCES dwh.dim_date(sale_date),
    customer_id INT NOT NULL REFERENCES dwh.dim_customer(customer_id),
    seller_id INT NOT NULL REFERENCES dwh.dim_seller(seller_id),
    product_id INT NOT NULL REFERENCES dwh.dim_product(product_id),
    store_name TEXT NOT NULL REFERENCES dwh.dim_store(store_name),
    supplier_name TEXT NOT NULL REFERENCES dwh.dim_supplier(supplier_name),
    sale_quantity INT,
    sale_total_price NUMERIC(12,2)
);
