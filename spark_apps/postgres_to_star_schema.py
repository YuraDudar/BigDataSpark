from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, expr, year, quarter, month, dayofmonth,
    dayofweek, weekofyear, row_number
)

# --- Application Configuration ---
APP_NAME = "PostgresToStarSchemaETL"
POSTGRES_JDBC_JAR_PATH = "/opt/spark/jars/postgresql-42.6.0.jar"

# --- Database Connection Details ---
DB_CONNECTION_URL = "jdbc:postgresql://postgres_db:5432/spark_db"
DB_PROPERTIES = {
    "user": "spark_user_name",
    "password": "spark_my_secret_password",
    "driver": "org.postgresql.Driver"
}
RAW_DATA_TABLE = "mock_data"
JDBC_WRITE_MODE = "append"


def initialize_spark_session(app_name, jar_path):
    """Initializes and returns a SparkSession."""
    print(f"Initializing Spark session: {app_name} with JAR: {jar_path}")
    session = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.jars", jar_path)
        .getOrCreate()
    )
    print("Spark session initialized.")
    return session


def load_dataframe_from_postgres(spark, url, table, properties):
    """Loads a DataFrame from a PostgreSQL table."""
    print(f"Loading data from PostgreSQL table: {table}")
    df = (
        spark.read
        .format("jdbc")
        .option("url", url)
        .option("dbtable", table)
        .option("user", properties["user"])
        .option("password", properties["password"])
        .option("driver", properties["driver"])
        .load()
    )
    print(f"Loaded {df.count()} rows from {table}.")
    return df


def save_dataframe_to_postgres(df, url, table, mode, properties):
    """Saves a DataFrame to a PostgreSQL table."""
    print(f"Saving {df.count()} rows to PostgreSQL table: {table} (mode: {mode})")
    (
        df.write
        .format("jdbc")
        .option("url", url)
        .option("dbtable", table)
        .option("user", properties["user"])
        .option("password", properties["password"])
        .option("driver", properties["driver"])
        .mode(mode)
        .save()
    )
    print(f"Successfully saved data to {table}.")


def process_dimension_table(source_df, id_col, order_col, attributes_map, target_table_name):
    """
    Extracts, transforms, and loads a dimension table.
    Uses a window function to pick the most recent attributes based on order_col (SCD Type 1).
    """
    print(f"Processing dimension table: {target_table_name}")

    cols_to_select = [id_col, order_col] + list(attributes_map.keys())
    distinct_cols_to_select = sorted(list(set(cols_to_select)))
    dim_df = source_df.select(*distinct_cols_to_select)
    window_spec = Window.partitionBy(col(id_col)).orderBy(col(order_col).desc())

    processed_dim_df = (
        dim_df
        .withColumn("rn", row_number().over(window_spec))
        .where(col("rn") == 1)
        .drop("rn", order_col)
    )

    for source_name, target_name in attributes_map.items():
        processed_dim_df = processed_dim_df.withColumnRenamed(source_name, target_name)

    save_dataframe_to_postgres(processed_dim_df, DB_CONNECTION_URL, target_table_name, JDBC_WRITE_MODE, DB_PROPERTIES)
    return processed_dim_df  # Возвращаем для возможного использования (хотя для SK мы перечитываем)


def run_etl():
    """Main ETL process."""
    spark = initialize_spark_session(APP_NAME, POSTGRES_JDBC_JAR_PATH)

    # 1. Load source data from mock_data table
    source_dataframe = load_dataframe_from_postgres(spark, DB_CONNECTION_URL, RAW_DATA_TABLE, DB_PROPERTIES)
    source_dataframe.cache()  # Cache for multiple uses

    # 2. Process and load Dimension Tables
    process_dimension_table(
        source_df=source_dataframe,
        id_col="sale_customer_id",
        order_col="sale_date",
        attributes_map={
            "sale_customer_id": "customer_id",
            "customer_first_name": "first_name",
            "customer_last_name": "last_name",
            "customer_age": "age",
            "customer_email": "email",
            "customer_country": "country",
            "customer_postal_code": "postal_code"
        },
        target_table_name="dim_customer"
    )

    # dim_seller
    process_dimension_table(
        source_df=source_dataframe,
        id_col="sale_seller_id",
        order_col="sale_date",
        attributes_map={
            "sale_seller_id": "seller_id",
            "seller_first_name": "first_name",
            "seller_last_name": "last_name",
            "seller_email": "email",
            "seller_country": "country",
            "seller_postal_code": "postal_code"
        },
        target_table_name="dim_seller"
    )

    # dim_product
    process_dimension_table(
        source_df=source_dataframe,
        id_col="sale_product_id",
        order_col="sale_date",
        attributes_map={
            "sale_product_id": "product_id",
            "product_name": "name",
            "product_category": "category",
            "product_weight": "weight",
            "product_color": "color",
            "product_size": "size",
            "product_brand": "brand",
            "product_material": "material",
            "product_description": "description",
            "product_rating": "rating",
            "product_reviews": "reviews",
            "product_release_date": "release_date",
            "product_expiry_date": "expiry_date",
            "product_price": "unit_price"
        },
        target_table_name="dim_product"
    )

    # dim_store
    process_dimension_table(
        source_df=source_dataframe,
        id_col="store_name",
        order_col="sale_date",
        attributes_map={
            "store_name": "name",
            "store_location": "location",
            "store_city": "city",
            "store_state": "state",
            "store_country": "country",
            "store_phone": "phone",
            "store_email": "email"
        },
        target_table_name="dim_store"
    )

    # dim_supplier
    process_dimension_table(
        source_df=source_dataframe,
        id_col="supplier_name",
        order_col="sale_date",
        attributes_map={
            "supplier_name": "name",
            "supplier_contact": "contact",
            "supplier_email": "email",
            "supplier_phone": "phone",
            "supplier_address": "address",
            "supplier_city": "city",
            "supplier_country": "country"
        },
        target_table_name="dim_supplier"
    )

    # dim_date
    print("Processing dimension table: dim_date")
    dim_date_df = (
        source_dataframe
        .select(col("sale_date"))
        .filter(col("sale_date").isNotNull())
        .distinct()
        .withColumn("year", year(col("sale_date")))
        .withColumn("quarter", quarter(col("sale_date")))
        .withColumn("month", month(col("sale_date")))
        .withColumn("month_name", expr("date_format(sale_date, 'MMMM')"))
        .withColumn("day_of_month", dayofmonth(col("sale_date")))
        .withColumn("day_of_week", dayofweek(col("sale_date")))
        .withColumn("week_of_year", weekofyear(col("sale_date")))
        .withColumn("is_weekend", dayofweek(col("sale_date")).isin([1, 7]))
    )
    save_dataframe_to_postgres(dim_date_df, DB_CONNECTION_URL, "dim_date", JDBC_WRITE_MODE, DB_PROPERTIES)

    # 3. Load Dimension tables back to get their generated surrogate keys (_sk)
    print("Reloading dimension tables to get surrogate keys.")
    customer_dim_sk = load_dataframe_from_postgres(spark, DB_CONNECTION_URL, "dim_customer", DB_PROPERTIES).select(
        "customer_sk", "customer_id")
    seller_dim_sk = load_dataframe_from_postgres(spark, DB_CONNECTION_URL, "dim_seller", DB_PROPERTIES).select(
        "seller_sk", "seller_id")
    product_dim_sk = load_dataframe_from_postgres(spark, DB_CONNECTION_URL, "dim_product", DB_PROPERTIES).select(
        "product_sk", "product_id")
    store_dim_sk = load_dataframe_from_postgres(spark, DB_CONNECTION_URL, "dim_store", DB_PROPERTIES).select("store_sk",
                                                                                                             "name")
    supplier_dim_sk = load_dataframe_from_postgres(spark, DB_CONNECTION_URL, "dim_supplier", DB_PROPERTIES).select(
        "supplier_sk", "name")
    date_dim_sk = load_dataframe_from_postgres(spark, DB_CONNECTION_URL, "dim_date", DB_PROPERTIES).select("date_sk",
                                                                                                           "sale_date")

    # 4. Process and load Fact Table
    print("Processing fact table: fact_sales")
    fact_sales_df = (
        source_dataframe
        .join(date_dim_sk, source_dataframe.sale_date == date_dim_sk.sale_date, "inner")
        .join(customer_dim_sk, source_dataframe.sale_customer_id == customer_dim_sk.customer_id, "inner")
        .join(seller_dim_sk, source_dataframe.sale_seller_id == seller_dim_sk.seller_id, "inner")
        .join(product_dim_sk, source_dataframe.sale_product_id == product_dim_sk.product_id, "inner")
        .join(store_dim_sk, source_dataframe.store_name == store_dim_sk.name, "inner")
        .join(supplier_dim_sk, source_dataframe.supplier_name == supplier_dim_sk.name, "inner")
        .select(
            date_dim_sk.date_sk,
            customer_dim_sk.customer_sk,
            seller_dim_sk.seller_sk,
            product_dim_sk.product_sk,
            store_dim_sk.store_sk,
            supplier_dim_sk.supplier_sk,
            source_dataframe.sale_quantity,
            source_dataframe.sale_total_price,
            source_dataframe.product_price.alias("transaction_unit_price")
        )
    )
    save_dataframe_to_postgres(fact_sales_df, DB_CONNECTION_URL, "fact_sales", JDBC_WRITE_MODE, DB_PROPERTIES)

    source_dataframe.unpersist()  # Release cache
    print("ETL process to Star Schema in PostgreSQL finished successfully.")
    spark.stop()
    print("Spark session stopped.")


if __name__ == "__main__":
    run_etl()