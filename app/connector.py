import logging

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

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

    def execute(self, command: str) -> None:
        if self.conn:
            cur = self.conn.cursor()
            cur.execute(command)
            cur.close()
        else:
            logger.exception("No connection present")

    def close(self):
        self.conn.close()
