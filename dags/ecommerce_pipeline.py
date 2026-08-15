from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime, timedelta

# Базові налаштування
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'azure_to_snowflake_pipeline',
    default_args=default_args,
    description='Завантаження сирих JSON файлів з Azure та трансформація у Snowflake',
    schedule_interval=timedelta(minutes=5), # Запуск кожні 5 хвилин
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['ecommerce', 'real-time'],
) as dag:

    # 1. Завдання: Скопіювати нові файли з Azure у сиру таблицю
    # Snowflake автоматично відстежує, які файли він вже завантажував, тому дублікатів не буде
    load_to_raw_table = SnowflakeOperator(
        task_id='load_azure_files_to_raw',
        snowflake_conn_id='snowflake_default',
        sql="""
            COPY INTO ecommerce_db.raw.sales_data(raw_json)
            FROM @ecommerce_db.raw.azure_sales_stage
            FILE_FORMAT = (TYPE = JSON, STRIP_OUTER_ARRAY = TRUE);
        """
    )

    # 2. Завдання: Трансформація JSON у нормальну реляційну таблицю (View)
    # Ми витягуємо дані з JSON-структури за допомогою синтаксису ":"
    create_analytics_view = SnowflakeOperator(
        task_id='create_analytics_view',
        snowflake_conn_id='snowflake_default',
        sql="""
            CREATE OR REPLACE VIEW ecommerce_db.raw.sales_analytics AS
            SELECT 
                raw_json:transaction_id::STRING AS transaction_id,
                raw_json:user_id::INTEGER AS user_id,
                raw_json:product_name::STRING AS product_name,
                raw_json:price::FLOAT AS price,
                raw_json:timestamp::TIMESTAMP AS transaction_time
            FROM ecommerce_db.raw.sales_data;
        """
    )

    # Встановлюємо порядок виконання: спочатку завантаження, потім оновлення вітрини
    load_to_raw_table >> create_analytics_view