from pyspark.sql import SparkSession

SPARK_APP_NAME = "emesa-DE-case"
SPARK_CLUSTER_MODE = "local[1]"
SPARK_POSTGRES_DRIVER_PATH = "./app/spark-drivers/postgresql-42.3.7.jar"

spark_session = (
    SparkSession.builder.master(SPARK_CLUSTER_MODE)
    .appName(SPARK_APP_NAME)
    .config("spark.jars", SPARK_POSTGRES_DRIVER_PATH)
    .getOrCreate()
)
