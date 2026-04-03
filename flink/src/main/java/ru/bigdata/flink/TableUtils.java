package ru.bigdata.flink;

public class TableUtils {

    public static String DROP_SQEMA_DDL = "DROP SCHEMA snowflake CASCADE;";
    public static String CREATE_SQEMA_DDL = "CREATE SCHEMA IF NOT EXISTS snowflake;";

    public static String DIM_PET_TYPES_DDL = "CREATE TABLE IF NOT EXISTS snowflake.dim_pet_types (\n" +
            "    pet_type_id SERIAL PRIMARY KEY,\n" +
            "    name VARCHAR(50),\n" +
            "    CONSTRAINT unique_name_pet_types UNIQUE (name)\n" + 
            ");";

    public static String DIM_TEP_BREEDS_DDL = "CREATE TABLE IF NOT EXISTS snowflake.dim_pet_breeds (\n" +
            "    pet_breed_id SERIAL PRIMARY KEY,\n" +
            "    pet_type_id INTEGER REFERENCES snowflake.dim_pet_types(pet_type_id) ON DELETE SET NULL,\n" +
            "    name VARCHAR(100),\n" +
            "    CONSTRAINT unique_name_pet_breeds UNIQUE (name, pet_type_id)  -- Уникальность по name и pet_type_id\n" +
            ");";

    public static String DIM_PETS_DDL = "CREATE TABLE IF NOT EXISTS snowflake.dim_pets (\n" +
            "    pet_id SERIAL PRIMARY KEY,\n" +
            "    name VARCHAR(100),\n" +
            "    breed_id INTEGER REFERENCES snowflake.dim_pet_breeds(pet_breed_id) ON DELETE SET NULL,\n" +
            "    CONSTRAINT unique_name_breed_pet UNIQUE (name, breed_id)\n" +  
            ");";

    public static String DIM_COUNTRIES_DDL = "CREATE TABLE IF NOT EXISTS snowflake.dim_countries (\n" +
            "    country_id SERIAL PRIMARY KEY,\n" +
            "    name VARCHAR(100),\n" +
            "    CONSTRAINT unique_name_countries UNIQUE (name)  -- Уникальность по name\n" +
            ");";

    public static String DIM_STATES_DDL = "CREATE TABLE IF NOT EXISTS snowflake.dim_states (\n" +
            "    state_id SERIAL PRIMARY KEY,\n" +
            "    name VARCHAR(100),\n" +
            "    CONSTRAINT unique_name_states UNIQUE (name)  -- Уникальность по name\n" +
            ");";

    public static String DIM_CITIES_DDL = "CREATE TABLE IF NOT EXISTS snowflake.dim_cities (\n" +
            "    city_id SERIAL PRIMARY KEY,\n" +
            "    name VARCHAR(100),\n" +
            "    CONSTRAINT unique_name_cities UNIQUE (name)  -- Уникальность по name\n" +
            ");";

    public static String DIM_CUSTOMERS_DDL = "CREATE TABLE IF NOT EXISTS snowflake.dim_customers (\n" +
            "    customer_id SERIAL PRIMARY KEY,\n" +
            "    first_name VARCHAR(100),\n" +
            "    last_name VARCHAR(100),\n" +
            "    email VARCHAR(255),\n" +
            "    age INTEGER,\n" +
            "    country_id INTEGER REFERENCES snowflake.dim_countries(country_id) ON DELETE SET NULL,\n" +
            "    postal_code VARCHAR(20),\n" +
            "    pet_id INTEGER REFERENCES snowflake.dim_pets(pet_id) ON DELETE SET NULL\n" +
            ");";

    public static String DIM_SELLERS_DDL = "CREATE TABLE IF NOT EXISTS snowflake.dim_sellers (\n" +
            "    seller_id SERIAL PRIMARY KEY,\n" +
            "    first_name VARCHAR(100),\n" +
            "    last_name VARCHAR(100),\n" +
            "    email VARCHAR(255) UNIQUE,\n" +  
            "    country_id INTEGER REFERENCES snowflake.dim_countries(country_id) ON DELETE SET NULL,\n" +
            "    postal_code VARCHAR(20)\n" +
            ");";

    public static String DIM_STORES_DDL = "CREATE TABLE IF NOT EXISTS snowflake.dim_stores (\n" +
            "    store_id SERIAL PRIMARY KEY,\n" +
            "    name VARCHAR(255) UNIQUE,\n" + 
            "    location VARCHAR(255),\n" +
            "    country_id INTEGER REFERENCES snowflake.dim_countries(country_id) ON DELETE SET NULL,\n" +
            "    state_id INTEGER REFERENCES snowflake.dim_states(state_id) ON DELETE SET NULL,\n" +
            "    city_id INTEGER REFERENCES snowflake.dim_cities(city_id) ON DELETE SET NULL,\n" +
            "    phone VARCHAR(50),\n" +
            "    email VARCHAR(255)\n" +
            ");";

    public static String DIM_PRODUCT_CATEGORIES = "CREATE TABLE IF NOT EXISTS snowflake.dim_product_categories (\n" +
            "    category_id SERIAL PRIMARY KEY,\n" +
            "    name VARCHAR(100),\n" +
            "    CONSTRAINT unique_name_product_categories UNIQUE (name)  -- Уникальность по name\n" +
            ");";

    public static String DIM_PET_CATEGORIES_DDL = "CREATE TABLE IF NOT EXISTS snowflake.dim_pet_categories (\n" +
            "    pet_category_id SERIAL PRIMARY KEY,\n" +
            "    name VARCHAR(50),\n" +
            "    CONSTRAINT unique_name_pet_categories UNIQUE (name)  -- Уникальность по name\n" +
            ");";

    public static String DIM_SUPPLIERS_DDL = "CREATE TABLE IF NOT EXISTS snowflake.dim_suppliers (\n" +
            "    supplier_id SERIAL PRIMARY KEY,\n" +
            "    name VARCHAR(255),\n" +
            "    contact VARCHAR(255),\n" +
            "    email VARCHAR(255) UNIQUE,\n" +  
            "    phone VARCHAR(50),\n" +
            "    address TEXT,\n" +
            "    city_id INTEGER REFERENCES snowflake.dim_cities(city_id) ON DELETE SET NULL,\n" +
            "    country_id INTEGER REFERENCES snowflake.dim_countries(country_id) ON DELETE SET NULL\n" +
            ");";

    public static String DIM_COLORS_DDL = "CREATE TABLE IF NOT EXISTS snowflake.dim_colors (\n" +
            "    color_id SERIAL PRIMARY KEY,\n" +
            "    name VARCHAR(50),\n" +
            "    CONSTRAINT unique_name_colors UNIQUE (name)  -- Уникальность по name\n" +
            ");";

    public static String DIM_BRANDS_DDL = "CREATE TABLE IF NOT EXISTS snowflake.dim_brands (\n" +
            "    brand_id SERIAL PRIMARY KEY,\n" +
            "    name VARCHAR(100),\n" +
            "    CONSTRAINT unique_name_brands UNIQUE (name)  -- Уникальность по name\n" +
            ");";

    public static String DIM_MATERIALS_DDL = "CREATE TABLE IF NOT EXISTS snowflake.dim_materials (\n" +
            "    material_id SERIAL PRIMARY KEY,\n" +
            "    name VARCHAR(100),\n" +
            "    CONSTRAINT unique_name_materials UNIQUE (name)  -- Уникальность по name\n" +
            ");";

    public static String DIM_PRODUCTS_DDL = "CREATE TABLE IF NOT EXISTS snowflake.dim_products (\n" +
            "    product_id SERIAL PRIMARY KEY,\n" +
            "    name VARCHAR(255),\n" +
            "    pet_category_id INTEGER REFERENCES snowflake.dim_pet_categories(pet_category_id) ON DELETE SET NULL,\n" +
            "    category_id INTEGER REFERENCES snowflake.dim_product_categories(category_id) ON DELETE SET NULL,\n" +
            "    price DECIMAL(10,2),\n" +
            "    weight DECIMAL(10,2),\n" +
            "    color_id INTEGER REFERENCES snowflake.dim_colors(color_id) ON DELETE SET NULL,\n" +
            "    size VARCHAR(50),\n" +
            "    brand_id INTEGER REFERENCES snowflake.dim_brands(brand_id) ON DELETE SET NULL,\n" +
            "    material_id INTEGER REFERENCES snowflake.dim_materials(material_id) ON DELETE SET NULL,\n" +
            "    description TEXT,\n" +
            "    rating DECIMAL(3,1),\n" +
            "    reviews INTEGER,\n" +
            "    release_date DATE,\n" +
            "    expiry_date DATE,\n" +
            "    supplier_id INTEGER REFERENCES snowflake.dim_suppliers(supplier_id) ON DELETE SET NULL\n" +
            ");";

    public static String FACT_SALES_DDL = "CREATE TABLE IF NOT EXISTS snowflake.fact_sales (\n" +
            "    sale_id SERIAL PRIMARY KEY,\n" +
            "    customer_id INTEGER REFERENCES snowflake.dim_customers(customer_id) ON DELETE SET NULL,\n" +
            "    seller_id INTEGER REFERENCES snowflake.dim_sellers(seller_id) ON DELETE SET NULL,\n" +
            "    product_id INTEGER REFERENCES snowflake.dim_products(product_id) ON DELETE SET NULL,\n" +
            "    store_id INTEGER REFERENCES snowflake.dim_stores(store_id) ON DELETE SET NULL,\n" +
            "    quantity INTEGER,\n" +
            "    total_price DECIMAL(10,2),\n" +
            "    date DATE\n" +
            ");";
}
