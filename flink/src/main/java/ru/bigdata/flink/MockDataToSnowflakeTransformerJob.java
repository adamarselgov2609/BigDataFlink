package ru.bigdata.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import ru.bigdata.flink.configuration.FlinkConnectionUtils;
import ru.bigdata.flink.dto.MockDataDto;
import ru.bigdata.flink.mapper.JsonToDtoMapper;


import static ru.bigdata.flink.UploadDataUtils.*;
import static ru.bigdata.flink.TableUtils.*;

public class MockDataToSnowflakeTransformerJob {

    final static String inputTopic = "toxis";
    final static String jobTitle = "KafkaToPostgresTest";

    public static void main(String[] args) throws Exception {
        final String bootstrapServers = "localhost:29092";
        final String jdbcUrl = "jdbc:postgresql://localhost:5438/db";
        final String jdbcUser = "user";
        final String jdbcPassword = "password";

        FlinkConnectionUtils flinkConnectionUtils = new FlinkConnectionUtils(bootstrapServers, inputTopic, jdbcUrl, jdbcUser, jdbcPassword);
        PostgresCommandsExecutor commandsExecutor = new PostgresCommandsExecutor(jdbcUrl, jdbcUser, jdbcPassword);

        JdbcConnectionOptions postgresConnectorOptions = flinkConnectionUtils.getPostgresConnectionOptions();
        KafkaSource<String> source = flinkConnectionUtils.getKafkaSource();

        executeSchemaSetup(commandsExecutor);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> text = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
        DataStream<MockDataDto> dtoDataStream = text.map(new JsonToDtoMapper());


        dtoDataStream.addSink(
                JdbcSink.sink(
                        "INSERT INTO snowflake.dim_pet_types (name) VALUES (?) ON CONFLICT (name) DO NOTHING",
                        (ps, t) -> ps.setString(1, t.getCustomer_pet_type()),
                        JdbcExecutionOptions.builder().withBatchSize(1).withBatchIntervalMs(0).build(),
                        postgresConnectorOptions
                )
        );
        dtoDataStream.addSink(
                JdbcSink.sink(
                        "INSERT INTO snowflake.dim_pet_breeds (name, pet_type_id) VALUES (?, ?) ON CONFLICT (name, pet_type_id) DO NOTHING",
                        (ps, t) -> {
                            int petTypeId = getOrCreatePetTypeId(t.getCustomer_pet_type());
                            System.out.println("ID типа питомца: " + petTypeId  + "тип питомца " + t.getCustomer_pet_type());
                            ps.setString(1, t.getCustomer_pet_breed());
                            ps.setInt(2, petTypeId);
                        },
                        JdbcExecutionOptions.builder().withBatchSize(1).withBatchIntervalMs(0).build(),
                        postgresConnectorOptions
                )
        );

        dtoDataStream.addSink(
                JdbcSink.sink(
                        "INSERT INTO snowflake.dim_pets (name, breed_id) VALUES (?, ?)",
                        (ps, t) -> {
                            int petBreedId = getOrCreatePetBreedId(t.getCustomer_pet_breed());
                            ps.setString(1, t.getCustomer_pet_name());
                            ps.setInt(2, petBreedId);
                        },
                        JdbcExecutionOptions.builder().withBatchSize(1).withBatchIntervalMs(0).build(),
                        postgresConnectorOptions
                )
        );

        dtoDataStream.addSink(
                JdbcSink.sink(
                        "INSERT INTO snowflake.dim_countries (name) VALUES (?) ON CONFLICT (name) DO NOTHING",
                        (ps, t) -> ps.setString(1, t.getCustomer_country()),
                        JdbcExecutionOptions.builder().withBatchSize(1).withBatchIntervalMs(0).build(),
                        postgresConnectorOptions
                )
        );

        dtoDataStream.addSink(
                JdbcSink.sink(
                        "INSERT INTO snowflake.dim_cities (name) VALUES (?) ON CONFLICT (name) DO NOTHING",
                        (ps, t) -> ps.setString(1, t.getStore_city()), // Используем store_city
                        JdbcExecutionOptions.builder().withBatchSize(1).withBatchIntervalMs(0).build(),
                        postgresConnectorOptions
                )
        );

        dtoDataStream.addSink(
                JdbcSink.sink(
                        "INSERT INTO snowflake.dim_states (name) VALUES (?) ON CONFLICT (name) DO NOTHING",
                        (ps, t) -> ps.setString(1, t.getStore_state()), // Используем store_state
                        JdbcExecutionOptions.builder().withBatchSize(1).withBatchIntervalMs(0).build(),
                        postgresConnectorOptions
                )
        );

        dtoDataStream.addSink(
                JdbcSink.sink(
                        "INSERT INTO snowflake.dim_customers (first_name, last_name, email, age, country_id, postal_code, pet_id) " +
                                "VALUES (?, ?, ?, ?, ?, ?, ?) " +
                                "ON CONFLICT DO NOTHING",
                        (ps, t) -> {
                            Integer countryId = getOrCreateCountryId(t.getCustomer_country());
                            Integer petId = getOrCreatePetId(t.getCustomer_pet_name());
                            ps.setString(1, t.getCustomer_first_name());
                            ps.setString(2, t.getCustomer_last_name());
                            ps.setString(3, t.getCustomer_email());
                            ps.setInt(4, t.getCustomer_age());
                            ps.setInt(5, countryId);
                            ps.setString(6, t.getCustomer_postal_code());
                            ps.setInt(7, petId);
                        },
                        JdbcExecutionOptions.builder().withBatchSize(1).withBatchIntervalMs(0).build(),
                        postgresConnectorOptions
                )
        );

        dtoDataStream.addSink(
                JdbcSink.sink(
                        "INSERT INTO snowflake.dim_sellers (first_name, last_name, email, country_id, postal_code) VALUES (?, ?, ?, ?, ?) ON CONFLICT (email) DO NOTHING",
                        (ps, t) -> {
                            int countryId = getOrCreateCountryId(t.getSeller_country());
                            ps.setString(1, t.getSeller_first_name());
                            ps.setString(2, t.getSeller_last_name());
                            ps.setString(3, t.getSeller_email());
                            ps.setInt(4, countryId);
                            ps.setString(5, t.getSeller_postal_code());
                        },
                        JdbcExecutionOptions.builder().withBatchSize(1).withBatchIntervalMs(0).build(),
                        postgresConnectorOptions
                )
        );

        dtoDataStream.addSink(
                JdbcSink.sink(
                        "INSERT INTO snowflake.dim_stores (name, location, country_id, state_id, city_id, phone, email) VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT (name) DO NOTHING",
                        (ps, t) -> {
                            int countryId = getOrCreateCountryId(t.getStore_country());
                            int stateId = getOrCreateStateId(t.getStore_state());
                            int cityId = getOrCreateCityId(t.getStore_city());
                            ps.setString(1, t.getStore_name());
                            ps.setString(2, t.getStore_location());
                            ps.setInt(3, countryId);
                            ps.setInt(4, stateId);
                            ps.setInt(5, cityId);
                            ps.setString(6, t.getStore_phone());
                            ps.setString(7, t.getStore_email());
                        },
                        JdbcExecutionOptions.builder().withBatchSize(1).withBatchIntervalMs(0).build(),
                        postgresConnectorOptions
                )
        );

        dtoDataStream.addSink(
                JdbcSink.sink(
                        "INSERT INTO snowflake.dim_product_categories (name) VALUES (?) ON CONFLICT (name) DO NOTHING",
                        (ps, t) -> ps.setString(1, t.getProduct_category()),
                        JdbcExecutionOptions.builder().withBatchSize(1).withBatchIntervalMs(0).build(),
                        postgresConnectorOptions
                )
        );

        dtoDataStream.addSink(
                JdbcSink.sink(
                        "INSERT INTO snowflake.dim_pet_categories (name) VALUES (?) ON CONFLICT (name) DO NOTHING",
                        (ps, t) -> ps.setString(1, t.getPet_category()),
                        JdbcExecutionOptions.builder().withBatchSize(1).withBatchIntervalMs(0).build(),
                        postgresConnectorOptions
                )
        );

        dtoDataStream.addSink(
                JdbcSink.sink(
                        "INSERT INTO snowflake.dim_suppliers (name, contact, email, phone, address, city_id, country_id) VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT (email) DO NOTHING",
                        (ps, t) -> {
                            int cityId = getOrCreateCityId(t.getSupplier_city());
                            int countryId = getOrCreateCountryId(t.getSupplier_country());
                            ps.setString(1, t.getSupplier_name());
                            ps.setString(2, t.getSupplier_contact());
                            ps.setString(3, t.getSupplier_email());
                            ps.setString(4, t.getSupplier_phone());
                            ps.setString(5, t.getSupplier_address());
                            ps.setInt(6, cityId);
                            ps.setInt(7, countryId);
                        },
                        JdbcExecutionOptions.builder().withBatchSize(1).withBatchIntervalMs(0).build(),
                        postgresConnectorOptions
                )
        );

        dtoDataStream.addSink(
                JdbcSink.sink(
                        "INSERT INTO snowflake.dim_colors (name) VALUES (?) ON CONFLICT (name) DO NOTHING",
                        (ps, t) -> ps.setString(1, t.getProduct_color()),
                        JdbcExecutionOptions.builder().withBatchSize(1).withBatchIntervalMs(0).build(),
                        postgresConnectorOptions
                )
        );

        dtoDataStream.addSink(
                JdbcSink.sink(
                        "INSERT INTO snowflake.dim_brands (name) VALUES (?) ON CONFLICT (name) DO NOTHING",
                        (ps, t) -> ps.setString(1, t.getProduct_brand()),
                        JdbcExecutionOptions.builder().withBatchSize(1).withBatchIntervalMs(0).build(),
                        postgresConnectorOptions
                )
        );

        dtoDataStream.addSink(
                JdbcSink.sink(
                        "INSERT INTO snowflake.dim_materials (name) VALUES (?) ON CONFLICT (name) DO NOTHING",
                        (ps, t) -> ps.setString(1, t.getProduct_material()),
                        JdbcExecutionOptions.builder().withBatchSize(1).withBatchIntervalMs(0).build(),
                        postgresConnectorOptions
                )
        );

        dtoDataStream.addSink(
                JdbcSink.sink(
                        "INSERT INTO snowflake.dim_products (name, pet_category_id, category_id, price, weight, color_id, size, brand_id, material_id, description, rating, reviews, release_date, expiry_date, supplier_id) " +
                                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (ps, t) -> {
                            int petCategoryId = getOrCreatePetCategoryId(t.getPet_category());
                            int categoryId = getOrCreateProductCategoryId(t.getProduct_category());
                            int colorId = getOrCreateColorId(t.getProduct_color());
                            int brandId = getOrCreateBrandId(t.getProduct_brand());
                            int materialId = getOrCreateMaterialId(t.getProduct_material());
                            int supplierId = getOrCreateSupplierId(t.getSupplier_name());

                            ps.setString(1, t.getProduct_name());
                            ps.setInt(2, petCategoryId);
                            ps.setInt(3, categoryId);
                            ps.setDouble(4, t.getProduct_price());
                            ps.setDouble(5, t.getProduct_weight());
                            ps.setInt(6, colorId);
                            ps.setString(7, t.getProduct_size());
                            ps.setInt(8, brandId);
                            ps.setInt(9, materialId);
                            ps.setString(10, t.getProduct_description());
                            ps.setDouble(11, t.getProduct_rating());
                            ps.setInt(12, t.getProduct_reviews());
                            ps.setDate(13, convertToSqlDate(t.getProduct_release_date()));
                            ps.setDate(14, convertToSqlDate(t.getProduct_expiry_date()));
                            ps.setInt(15, supplierId);;
                        },
                        JdbcExecutionOptions.builder().withBatchSize(1).withBatchIntervalMs(0).build(),
                        postgresConnectorOptions
                )
        );

        dtoDataStream.addSink(
                JdbcSink.sink(
                        "INSERT INTO snowflake.fact_sales (customer_id, seller_id, product_id, store_id, quantity, total_price, date) VALUES (?, ?, ?, ?, ?, ?, TO_DATE(?, 'YYYY-MM-DD')) ON CONFLICT (sale_id) DO NOTHING",
                        (ps, t) -> {
                            int customerId = getOrCreateCustomerId(t.getCustomer_first_name() + " " + t.getCustomer_last_name());
                            int sellerId = getOrCreateSellerId(t.getSeller_email());
                            int productId = getOrCreateProductId(t.getProduct_name());
                            int storeId = getOrCreateStoreId(t.getStore_name());

                            String formattedDate = convertToDateFormat(t.getSale_date());

                            ps.setInt(1, customerId);
                            ps.setInt(2, sellerId);
                            ps.setInt(3, productId);
                            ps.setInt(4, storeId);
                            ps.setInt(5, t.getSale_quantity());
                            ps.setDouble(6, t.getSale_total_price());
                            ps.setString(7, formattedDate);
                        },
                        JdbcExecutionOptions.builder().withBatchSize(1).withBatchIntervalMs(0).build(),
                        postgresConnectorOptions
                )
        );

        env.execute(jobTitle);
    }

    private static void executeSchemaSetup(PostgresCommandsExecutor commandsExecutor) {
        try {
            commandsExecutor.executeSqlQuery(DROP_SQEMA_DDL);
            commandsExecutor.executeSqlQuery(CREATE_SQEMA_DDL);
            commandsExecutor.executeSqlQuery(DIM_PET_TYPES_DDL);
            commandsExecutor.executeSqlQuery(DIM_TEP_BREEDS_DDL);
            commandsExecutor.executeSqlQuery(DIM_PETS_DDL);
            commandsExecutor.executeSqlQuery(DIM_COUNTRIES_DDL);
            commandsExecutor.executeSqlQuery(DIM_STATES_DDL);
            commandsExecutor.executeSqlQuery(DIM_CITIES_DDL);
            commandsExecutor.executeSqlQuery(DIM_CUSTOMERS_DDL);
            commandsExecutor.executeSqlQuery(DIM_SELLERS_DDL);
            commandsExecutor.executeSqlQuery(DIM_STORES_DDL);
            commandsExecutor.executeSqlQuery(DIM_PRODUCT_CATEGORIES);
            commandsExecutor.executeSqlQuery(DIM_PET_CATEGORIES_DDL);
            commandsExecutor.executeSqlQuery(DIM_SUPPLIERS_DDL);
            commandsExecutor.executeSqlQuery(DIM_COLORS_DDL);
            commandsExecutor.executeSqlQuery(DIM_BRANDS_DDL);
            commandsExecutor.executeSqlQuery(DIM_MATERIALS_DDL);
            commandsExecutor.executeSqlQuery(DIM_PRODUCTS_DDL);
            commandsExecutor.executeSqlQuery(FACT_SALES_DDL);
        } catch (Exception e) {
            System.err.println("Ошибка при выполнении SQL DDL запросов: ");
            e.printStackTrace();
        }
    }
}
