# BigDataFlink

Лабораторная работа №3  
**Streaming processing с использованием Apache Flink**

## Запуск

```bash
docker compose up
```

После запуска автоматически:
- поднимаются PostgreSQL, Kafka, Zookeeper и Flink;
- создаётся topic `sales_raw`;
- данные из 10 CSV-файлов отправляются в Kafka;
- Flink job читает поток из Kafka;
- данные загружаются в PostgreSQL в модель **звезда**;
- в логах выводятся итоговые результаты загрузки.

## Результат

Итоговые counts после выполнения:

- `dim_date` — 364
- `dim_customer` — 1000
- `dim_customer_pet` — 1000
- `dim_seller` — 1000
- `dim_product` — 1000
- `dim_store` — 383
- `dim_supplier` — 383
- `fact_sales` — 10000

## Что реализовано

- чтение 10 CSV-файлов;
- преобразование строк CSV в JSON;
- отправка данных в Kafka topic `sales_raw`;
- потоковая обработка данных в Apache Flink;
- загрузка данных в PostgreSQL в модель **звезда**;
- сохранение 10000 строк в таблице фактов `dwh.fact_sales`.

## Структура проекта

```text
.
├── docker-compose.yml
├── README.md
├── kafka_producer/
│   └── producer.py
├── flink_job/
│   └── job.sql
├── sql/
│   └── 01_init.sql
├── исходные данные/
│   ├── MOCK_DATA.csv
│   ├── MOCK_DATA (1).csv
│   ├── MOCK_DATA (2).csv
│   ├── MOCK_DATA (3).csv
│   ├── MOCK_DATA (4).csv
│   ├── MOCK_DATA (5).csv
│   ├── MOCK_DATA (6).csv
│   ├── MOCK_DATA (7).csv
│   ├── MOCK_DATA (8).csv
│   └── MOCK_DATA (9).csv
└── flink_ui_running.png
```
