import csv
import json
import glob
import os
import re
from kafka import KafkaProducer

TOPIC_NAME = "sales_raw"
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:9094")


def get_file_index(file_name: str) -> int:
    base_name = os.path.basename(file_name)

    if base_name == "MOCK_DATA.csv":
        return 0

    match = re.search(r"MOCK_DATA \((\d+)\)\.csv", base_name)
    if match:
        return int(match.group(1))

    raise ValueError(f"Не удалось определить номер файла: {base_name}")


producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode("utf-8")
)

csv_files = sorted(glob.glob("исходные данные/*.csv"))

print(f"Найдено файлов: {len(csv_files)}")

message_count = 0

for file_name in csv_files:
    file_index = get_file_index(file_name)
    print(f"Читаю файл: {file_name} | file_index={file_index}")

    with open(file_name, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)

        for row in reader:
            original_id = int(row["id"])
            global_sale_id = file_index * 100000 + original_id

            row["source_file"] = os.path.basename(file_name)
            row["file_index"] = str(file_index)
            row["global_sale_id"] = str(global_sale_id)

            producer.send(TOPIC_NAME, value=row)
            message_count += 1

producer.flush()
producer.close()

print(f"Всего отправлено сообщений: {message_count}")
