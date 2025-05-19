# BigDataSpark - Лабораторная работа №2: ETL с помощью Spark

> Инструкция по запуску и проверке ETL-пайплайна на Spark, PostgreSQL и ClickHouse.

## Предварительные требования
*   Docker и Docker Compose
*   Git
*   Скачанные JDBC-драйверы в папке `./jars/`:
    *   `postgresql-42.6.0.jar`
    *   `clickhouse-jdbc-0.4.6.jar`

## Запуск проекта
1.  **Клонировать репозиторий:**
    ```bash
    git clone https://github.com/YuraDudar/BigDataSnowflake
    cd BigDataSnowflake
    ```

2.  **Запустить сервисы (PostgreSQL, Spark, ClickHouse):**
    Из корневой директории проекта:
    ```bash
    docker compose up -d
    ```
    *PostgreSQL будет инициализирован: staging-таблица `mock_data` будет создана и заполнена из CSV, таблицы DWH (звезда) будут созданы пустыми.*

3.  **Запустить ETL из PostgreSQL (staging) в PostgreSQL (звезда):**
    ```bash
    docker exec -it spark-master spark-submit \
      --master spark://spark-master:7077 \
      --deploy-mode client \
      --jars /opt/spark/jars/postgresql-42.6.0.jar \
      /opt/spark-apps/postgres_to_star_schema.py
    ```

4.  **Запустить ETL из PostgreSQL (звезда) в ClickHouse (витрины):**
    ```bash
    docker exec -it spark-master spark-submit \
      --master spark://spark-master:7077 \
      --deploy-mode client \
      --jars /opt/spark/jars/postgresql-42.6.0.jar,/opt/spark/jars/clickhouse-jdbc-0.4.6.jar \
      /opt/spark-apps/star_to_clickhouse_reports.py
    ```

## Проверка результатов

1.  **Проверить список таблиц-отчетов в ClickHouse:**
    ```bash
    docker exec -it clickhouse_db clickhouse-client --query "SHOW TABLES LIKE 'mart%';"
    ```
    *Должен отобразиться список таблиц, таких как `mart_top_10_selling_products`, и т.д.*

2.  **Просмотреть данные в одной из таблиц-отчетов (пример):**
    ```bash
    docker exec -it clickhouse_db clickhouse-client --query "SELECT * FROM mart_top_10_selling_products LIMIT 3;"
    ```

## Остановка проекта

1.  **Остановить и удалить контейнеры:**
    ```bash
    docker compose down
    ```

2.  **(Опционально) Удалить volumes с данными:**
    ```bash
    docker compose down -v
    ```

---
