import os
import json
from datetime import datetime
from confluent_kafka import Consumer
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

# Завантажуємо рядок підключення з .env файлу
load_dotenv()

# ==========================================
# 1. НАЛАШТУВАННЯ AZURE
# ==========================================
connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
blob_service_client = BlobServiceClient.from_connection_string(connection_string)
container_name = "raw-sales-data"

# ==========================================
# 2. НАЛАШТУВАННЯ KAFKA
# ==========================================
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'azure_upload_group',
    'auto.offset.reset': 'earliest' # Почати читати з найстаріших непрочитаних повідомлень
}
consumer = Consumer(conf)
topic_name = 'ecommerce_sales'
consumer.subscribe([topic_name])

# Параметри батчингу (розмір пакета даних)
BATCH_SIZE = 20
messages_batch = []

print(f"📥 Слухаємо топік '{topic_name}' та збираємо батчі по {BATCH_SIZE} повідомлень...")

try:
    while True:
        # Чекаємо нове повідомлення 1 секунду
        msg = consumer.poll(1.0) 
        
        if msg is None:
            continue
        if msg.error():
            print(f"⚠️ Помилка Kafka: {msg.error()}")
            continue
            
        # Декодуємо повідомлення
        record = json.loads(msg.value().decode('utf-8'))
        messages_batch.append(record)
        print(f"   ➕ Отримано транзакцію: {record['transaction_id']}")
        
        # ==========================================
        # 3. ВІДПРАВКА В AZURE (Якщо батч заповнений)
        # ==========================================
        if len(messages_batch) >= BATCH_SIZE:
            # Формуємо унікальне ім'я файлу на основі поточного часу
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            file_name = f"sales_batch_{timestamp}.json"
            
            # Перетворюємо масив повідомлень у зручний JSON-текст
            json_data = json.dumps(messages_batch, indent=2)
            
            # Завантажуємо файл у контейнер Azure
            blob_client = blob_service_client.get_blob_client(container=container_name, blob=file_name)
            blob_client.upload_blob(json_data, overwrite=True)
            
            print(f"✅ Батч успішно завантажено в Azure Blob Storage: {file_name}\n")
            
            # Очищаємо список для збору наступного батчу
            messages_batch.clear()

except KeyboardInterrupt:
    print("\n⏹️ Споживач зупинений.")
finally:
    consumer.close()