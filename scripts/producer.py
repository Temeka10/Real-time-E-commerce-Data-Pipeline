import json
import time
from faker import Faker
from confluent_kafka import Producer

# Ініціалізуємо генератор фейкових даних
fake = Faker()

# Налаштування підключення до нашої локальної Kafka
conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(conf)

topic_name = 'ecommerce_sales'

def delivery_report(err, msg):
    """Функція зворотного виклику для підтвердження доставки повідомлення"""
    if err is not None:
        print(f"❌ Помилка доставки: {err}")

print(f"🚀 Починаємо генерацію даних у топік '{topic_name}'... (Натисніть Ctrl+C для зупинки)")

try:
    while True:
        # 1. Генеруємо випадкову транзакцію
        transaction = {
            "transaction_id": fake.uuid4(),
            "user_id": fake.random_int(min=100, max=999),
            "product_name": fake.word(ext_word_list=['MacBook Pro', 'iPhone 15', 'AirPods', 'Magic Mouse', 'iPad Air']),
            "price": round(fake.random.uniform(50.0, 2500.0), 2),
            "timestamp": fake.iso8601()
        }

        # 2. Перетворюємо словник у JSON-рядок
        record_value = json.dumps(transaction)

        # 3. Відправляємо повідомлення в Kafka
        producer.produce(
            topic_name,
            value=record_value.encode('utf-8'),
            callback=delivery_report
        )
        
        # Обслуговування черги повідомлень (обов'язково для confluent_kafka)
        producer.poll(0)
        
        print(f"📦 Відправлено: {record_value}")
        
        # Робимо паузу 2 секунди перед наступною "покупкою"
        time.sleep(2)

except KeyboardInterrupt:
    print("\n⏹️ Зупинено користувачем.")
finally:
    # Чекаємо, поки всі повідомлення в черзі будуть гарантовано відправлені
    producer.flush()