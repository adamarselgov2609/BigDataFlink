import os
import time
import json
import csv
from confluent_kafka import Producer

config = {
    'bootstrap_servers': 'localhost:29092',
    'topic': 'toxis',
    'interval_seconds': 5,
    'batch_size': 10, 
    'data_folder': 'initial_data'
}

producer = Producer({'bootstrap.servers': config['bootstrap_servers']})

def delivery_report(err, msg):
    if err is not None:
        print(f'❌ Ошибка доставки: {err}')
    else:
        print(f'✅ Сообщение доставлено в {msg.topic()} [{msg.partition()}]')

def get_csv_files(folder):
    return sorted(
        [os.path.join(folder, f) for f in os.listdir(folder) if f.endswith(".csv")]
    )

def file_row_generator(file_paths):

    for file_path in file_paths:
        print(f"📂 Открыт файл: {file_path}")
        with open(file_path, newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                yield row

def main():
    print(f"🚀 Старт отправки сообщений в Kafka-топик '{config['topic']}'")
    csv_files = get_csv_files(config['data_folder'])
    row_gen = file_row_generator(csv_files)

    while True:
        sent_count = 0
        try:
            for _ in range(config['batch_size']):
                row = next(row_gen)
                message = json.dumps(row)
                producer.produce(config['topic'], message.encode('utf-8'), callback=delivery_report)
                sent_count += 1
        except StopIteration:
            print("✅ Все файлы пройдены. Повторно начинаем сначала...")
            row_gen = file_row_generator(csv_files)
            continue

        producer.flush()
        print(f"⏳ Отправлено {sent_count} сообщений. Пауза {config['interval_seconds']} секунд...\n")
        time.sleep(config['interval_seconds'])

if __name__ == "__main__":
    main()
