# Real-time-E-commerce-Data-Pipeline
Building Data Pipeline
graph LR
    subgraph Data Generation
        A[Python Script: Fake E-commerce] -->|JSON| B(Apache Kafka)
    end
    subgraph Data Lake
        B -->|Batch Upload| C{AWS S3 / Azure Blob}
    end
    subgraph Data Warehouse
        C -->|COPY INTO| D[(Snowflake)]
        D -->|dbt / SQL| E[Analytics Tables]
    end
    subgraph Orchestration
        F[Apache Airflow] -.->|Triggers Tasks| B
        F -.->|Monitors| C
        F -.->|Executes SQL| D
    end
