import json
from datetime import datetime

import psycopg2
from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema


def parse_date(date_str):
    if not date_str or str(date_str).strip() == "":
        return None
    return datetime.strptime(date_str.strip(), "%m/%d/%Y").date()


def to_int(value):
    if value is None or str(value).strip() == "":
        return None
    return int(float(value))


def to_float(value):
    if value is None or str(value).strip() == "":
        return None
    return float(value)


def clean_text(value):
    if value is None:
        return None
    value = str(value).strip()
    return value if value != "" else None


class PostgresSink:
    def open(self, runtime_context):
        self.conn = psycopg2.connect(
            host="postgres",
            port=5432,
            dbname="bigdata_flink",
            user="postgres",
            password="postgres"
        )
        self.conn.autocommit = True
        self.cur = self.conn.cursor()

    def invoke(self, value, context):
        row = json.loads(value)

        sale_id = to_int(row.get("id"))
        sale_date = parse_date(row.get("sale_date"))

        customer_id = to_int(row.get("sale_customer_id"))
        seller_id = to_int(row.get("sale_seller_id"))
        product_id = to_int(row.get("sale_product_id"))

        customer_age = to_int(row.get("customer_age"))
        product_price = to_float(row.get("product_price"))
        product_quantity = to_int(row.get("product_quantity"))
        sale_quantity = to_int(row.get("sale_quantity"))
        sale_total_price = to_float(row.get("sale_total_price"))
        product_weight = to_float(row.get("product_weight"))
        product_rating = to_float(row.get("product_rating"))
        product_reviews = to_int(row.get("product_reviews"))

        product_release_date = parse_date(row.get("product_release_date"))
        product_expiry_date = parse_date(row.get("product_expiry_date"))

        store_name = clean_text(row.get("store_name"))
        supplier_name = clean_text(row.get("supplier_name"))

        if sale_date is not None:
            self.cur.execute(
                """
                INSERT INTO dwh.dim_date (sale_date, day_num, month_num, year_num)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (sale_date) DO NOTHING
                """,
                (sale_date, sale_date.day, sale_date.month, sale_date.year)
            )

        if customer_id is not None:
            self.cur.execute(
                """
                INSERT INTO dwh.dim_customer (
                    customer_id,
                    customer_first_name,
                    customer_last_name,
                    customer_age,
                    customer_email,
                    customer_country,
                    customer_postal_code
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (customer_id) DO UPDATE SET
                    customer_first_name = EXCLUDED.customer_first_name,
                    customer_last_name = EXCLUDED.customer_last_name,
                    customer_age = EXCLUDED.customer_age,
                    customer_email = EXCLUDED.customer_email,
                    customer_country = EXCLUDED.customer_country,
                    customer_postal_code = EXCLUDED.customer_postal_code
                """,
                (
                    customer_id,
                    clean_text(row.get("customer_first_name")),
                    clean_text(row.get("customer_last_name")),
                    customer_age,
                    clean_text(row.get("customer_email")),
                    clean_text(row.get("customer_country")),
                    clean_text(row.get("customer_postal_code")),
                )
            )

            self.cur.execute(
                """
                INSERT INTO dwh.dim_customer_pet (
                    customer_id,
                    customer_pet_type,
                    customer_pet_name,
                    customer_pet_breed,
                    pet_category
                )
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (customer_id) DO UPDATE SET
                    customer_pet_type = EXCLUDED.customer_pet_type,
                    customer_pet_name = EXCLUDED.customer_pet_name,
                    customer_pet_breed = EXCLUDED.customer_pet_breed,
                    pet_category = EXCLUDED.pet_category
                """,
                (
                    customer_id,
                    clean_text(row.get("customer_pet_type")),
                    clean_text(row.get("customer_pet_name")),
                    clean_text(row.get("customer_pet_breed")),
                    clean_text(row.get("pet_category")),
                )
            )

        if seller_id is not None:
            self.cur.execute(
                """
                INSERT INTO dwh.dim_seller (
                    seller_id,
                    seller_first_name,
                    seller_last_name,
                    seller_email,
                    seller_country,
                    seller_postal_code
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (seller_id) DO UPDATE SET
                    seller_first_name = EXCLUDED.seller_first_name,
                    seller_last_name = EXCLUDED.seller_last_name,
                    seller_email = EXCLUDED.seller_email,
                    seller_country = EXCLUDED.seller_country,
                    seller_postal_code = EXCLUDED.seller_postal_code
                """,
                (
                    seller_id,
                    clean_text(row.get("seller_first_name")),
                    clean_text(row.get("seller_last_name")),
                    clean_text(row.get("seller_email")),
                    clean_text(row.get("seller_country")),
                    clean_text(row.get("seller_postal_code")),
                )
            )

        if product_id is not None:
            self.cur.execute(
                """
                INSERT INTO dwh.dim_product (
                    product_id,
                    product_name,
                    product_category,
                    product_price,
                    product_quantity,
                    product_weight,
                    product_color,
                    product_size,
                    product_brand,
                    product_material,
                    product_description,
                    product_rating,
                    product_reviews,
                    product_release_date,
                    product_expiry_date
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (product_id) DO UPDATE SET
                    product_name = EXCLUDED.product_name,
                    product_category = EXCLUDED.product_category,
                    product_price = EXCLUDED.product_price,
                    product_quantity = EXCLUDED.product_quantity,
                    product_weight = EXCLUDED.product_weight,
                    product_color = EXCLUDED.product_color,
                    product_size = EXCLUDED.product_size,
                    product_brand = EXCLUDED.product_brand,
                    product_material = EXCLUDED.product_material,
                    product_description = EXCLUDED.product_description,
                    product_rating = EXCLUDED.product_rating,
                    product_reviews = EXCLUDED.product_reviews,
                    product_release_date = EXCLUDED.product_release_date,
                    product_expiry_date = EXCLUDED.product_expiry_date
                """,
                (
                    product_id,
                    clean_text(row.get("product_name")),
                    clean_text(row.get("product_category")),
                    product_price,
                    product_quantity,
                    product_weight,
                    clean_text(row.get("product_color")),
                    clean_text(row.get("product_size")),
                    clean_text(row.get("product_brand")),
                    clean_text(row.get("product_material")),
                    clean_text(row.get("product_description")),
                    product_rating,
                    product_reviews,
                    product_release_date,
                    product_expiry_date,
                )
            )

        if store_name is not None:
            self.cur.execute(
                """
                INSERT INTO dwh.dim_store (
                    store_name,
                    store_location,
                    store_city,
                    store_state,
                    store_country,
                    store_phone,
                    store_email
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (store_name) DO UPDATE SET
                    store_location = EXCLUDED.store_location,
                    store_city = EXCLUDED.store_city,
                    store_state = EXCLUDED.store_state,
                    store_country = EXCLUDED.store_country,
                    store_phone = EXCLUDED.store_phone,
                    store_email = EXCLUDED.store_email
                """,
                (
                    store_name,
                    clean_text(row.get("store_location")),
                    clean_text(row.get("store_city")),
                    clean_text(row.get("store_state")),
                    clean_text(row.get("store_country")),
                    clean_text(row.get("store_phone")),
                    clean_text(row.get("store_email")),
                )
            )

        if supplier_name is not None:
            self.cur.execute(
                """
                INSERT INTO dwh.dim_supplier (
                    supplier_name,
                    supplier_contact,
                    supplier_email,
                    supplier_phone,
                    supplier_address,
                    supplier_city,
                    supplier_country
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (supplier_name) DO UPDATE SET
                    supplier_contact = EXCLUDED.supplier_contact,
                    supplier_email = EXCLUDED.supplier_email,
                    supplier_phone = EXCLUDED.supplier_phone,
                    supplier_address = EXCLUDED.supplier_address,
                    supplier_city = EXCLUDED.supplier_city,
                    supplier_country = EXCLUDED.supplier_country
                """,
                (
                    supplier_name,
                    clean_text(row.get("supplier_contact")),
                    clean_text(row.get("supplier_email")),
                    clean_text(row.get("supplier_phone")),
                    clean_text(row.get("supplier_address")),
                    clean_text(row.get("supplier_city")),
                    clean_text(row.get("supplier_country")),
                )
            )

        if all([
            sale_id is not None,
            sale_date is not None,
            customer_id is not None,
            seller_id is not None,
            product_id is not None,
            store_name is not None,
            supplier_name is not None
        ]):
            self.cur.execute(
                """
                INSERT INTO dwh.fact_sales (
                    sale_id,
                    sale_date,
                    customer_id,
                    seller_id,
                    product_id,
                    store_name,
                    supplier_name,
                    sale_quantity,
                    sale_total_price
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (sale_id) DO UPDATE SET
                    sale_date = EXCLUDED.sale_date,
                    customer_id = EXCLUDED.customer_id,
                    seller_id = EXCLUDED.seller_id,
                    product_id = EXCLUDED.product_id,
                    store_name = EXCLUDED.store_name,
                    supplier_name = EXCLUDED.supplier_name,
                    sale_quantity = EXCLUDED.sale_quantity,
                    sale_total_price = EXCLUDED.sale_total_price
                """,
                (
                    sale_id,
                    sale_date,
                    customer_id,
                    seller_id,
                    product_id,
                    store_name,
                    supplier_name,
                    sale_quantity,
                    sale_total_price,
                )
            )

    def close(self):
        if hasattr(self, "cur"):
            self.cur.close()
        if hasattr(self, "conn"):
            self.conn.close()


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers("kafka:29092") \
        .set_topics("sales_raw") \
        .set_group_id("flink_sales_group") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    ds = env.from_source(
        kafka_source,
        watermark_strategy=None,
        source_name="Kafka Source"
    )

    ds.add_sink(PostgresSink())

    env.execute("Kafka to PostgreSQL Star Schema")


if __name__ == "__main__":
    main()
