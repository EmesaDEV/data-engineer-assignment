import logging
from typing import Optional

import pandas as pd
import pandas.io.sql as psql
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from pyspark.sql.dataframe import DataFrame as SparkDataFrame

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DatabaseConnector:
    def __init__(self):
        pass


class PostgresConnector(DatabaseConnector):
    def __init__(self, host, database, user, password, **kwargs):
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.port: str = kwargs.get("port", "5432")
        self.conn = None
        super().__init__()

    def connect(self) -> None:
        self.conn = psycopg2.connect(
            host=self.host,
            database=self.database,
            user=self.user,
            password=self.password,
            port=self.port,
        )
        self.conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

    def execute(
        self, command: str, response: bool = False
    ) -> Optional[psycopg2.extensions.cursor]:
        if self.conn:
            cur = self.conn.cursor()
            cur.execute(command)
            if response:
                return cur
            else:
                cur.close()
        else:
            logger.exception("No connection present")

    def _query_to_pandas(self, sql) -> pd.DataFrame:
        return psql.read_sql(sql, self.conn)

    def ingest_spark_dataframe(self, df: SparkDataFrame, table) -> None:
        url = f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"
        properties = {
            "user": self.user,
            "password": self.password,
            "driver": "org.postgresql.Driver",
        }
        df.write.jdbc(url=url, table=table, mode="append", properties=properties)

    def close(self) -> None:
        self.conn.close()
