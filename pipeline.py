from pyspark.sql.types import (IntegerType, StringType, StructField,
                               StructType, TimestampType)

from app.connector import PostgresConnector
from app.sparkconfig import spark_session

PG_CONN = {
    "host": "localhost",
    "database": "postgres",
    "user": "postgres",
    "password": "supersecret",
    "port": "5433",
}

SAMPLE_DATA_PATH = "./data/sample.csv"

spark = spark_session

pgc = PostgresConnector(**PG_CONN)
pgc.connect()

# Initialize database for loading
sql = """
create schema if not exists raw;
create schema if not exists dwh;
create schema if not exists mrt;
drop table if exists raw.sample_data;
"""
pgc.execute(sql)

# Create staging table in raw 'area'
sql = """
create table raw.sample_data (
    order_id integer,
    source varchar,
    customer_id integer,
    payment_id integer,
    voucher_id integer,
    product_id integer,
    website varchar,
    order_status varchar,
    voucher_status varchar,
    payment_status varchar,
    order_date timestamp,
    payment_date timestamp
    );
"""
pgc.execute(sql)

# Extract CSV data
sample_data_schema = StructType(
    [
        StructField("order_id", IntegerType(), True),
        StructField("source", StringType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("payment_id", IntegerType(), True),
        StructField("voucher_id", IntegerType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("website", StringType(), True),
        StructField("order_status", StringType(), True),
        StructField("voucher_status", StringType(), True),
        StructField("payment_status", StringType(), True),
        StructField("order_date", TimestampType(), True),
        StructField("payment_date", TimestampType(), True),
    ]
)
df = (
    spark.read.options(header=True, delimiter=",")
    .schema(sample_data_schema)
    .csv(SAMPLE_DATA_PATH)
)

# Load data to Postgres
pgc.ingest_spark_dataframe(df=df, table="raw.sample_data")

# Transform data
# Build orders table in the 'dwh' layer
sql = """
create table if not exists dwh.order as (
select order_id, order_status, order_date, customer_id
from raw.sample_data
)
"""
pgc.execute(sql)

# Build christmas promotion mart in 'mrt' layer
sql = """
create or replace view mrt.christmas_promotion as (
select *
from dwh.order
where extract(month from order_date) = 12
and order_status = 'PAID'
)
"""
pgc.execute(sql)

# Build valentines promotion mart in 'mrt' layer
sql = """
create or replace view mrt.valentines_promotion as (
select *
from dwh.order
where extract(month from order_date) = 2
and order_status = 'PAID'
)
"""
pgc.execute(sql)
