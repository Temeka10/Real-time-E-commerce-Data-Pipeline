# 🚀 Near Real-Time E-commerce Data Pipeline

## 🎯 Бізнес-ціль проєкту (Business Value)
Сучасний e-commerce бізнес вимагає швидкого прийняття рішень на основі актуальних даних. Цей проєкт реалізує масштабований конвеєр даних (Data Pipeline), який збирає транзакції користувачів у реальному часі, надійно зберігає їх в Озері Даних (Data Lake) та автоматично трансформує в аналітичні вітрини. 

Побудована архітектура дозволяє бізнес-аналітикам будувати дашборди на свіжих даних з мінімальною затримкою (Near Real-Time), а компанії — легко масштабувати інфраструктуру під час пікових навантажень (наприклад, у "Чорну п'ятницю").

## 🛠 Стек технологій (Tech Stack)
* **Data Generation:** Python (`Faker`)
* **Message Broker / Streaming:** Apache Kafka (KRaft mode)
* **Data Lake:** Azure Blob Storage
* **Data Warehouse:** Snowflake (External Stages, COPY INTO, JSON parsing)
* **Orchestration:** Apache Airflow
* **Infrastructure:** Docker & Docker Compose (Containerization)

## 🏗 Архітектура (Architecture)
Дані рухаються за наступним маршрутом:
1. `producer.py` імітує потік транзакцій і відправляє JSON-повідомлення в Kafka-топік.
2. `consumer.py` зчитує повідомлення, формує мікро-батчі та відправляє їх в Azure Blob Storage.
3. **Apache Airflow** щоп'ять хвилин ініціює пайплайн, який дає команду Snowflake завантажити нові файли з Azure (`COPY INTO`) та розпарсити сирий масив JSON (`STRIP_OUTER_ARRAY`) у реляційне представлення (View) для аналітиків.
<img width="1027" height="266" alt="Screenshot 2026-08-15 at 00 51 18" src="https://github.com/user-attachments/assets/0d93ad30-9b30-44dd-b3b6-c21a6ce0285c" />



