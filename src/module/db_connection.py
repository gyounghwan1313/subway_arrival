
from typing import Union
import logging
import pandas as pd
import pandas.io.sql as psql

# logger = logging.getLogger(__name__)


class PostgreSQL(object):

    def __init__(self,
                 host,
                 port: Union[str, int],
                 database: str,
                 user: str,
                 password: str):

        import psycopg2 as pg
        self._connect = pg.connect(host=host,port=str(port),database=database,user=user,password=password)

    def sql_execute(self, query: str) -> None:
        self.cur = self._connect.cursor()
        self.cur.execute(query)
        self._connect.commit()
        self.cur.close()

    def sql_dataframe(self, query: str) -> pd.DataFrame:
        df = psql.read_sql_query(query, self._connect)
        return df

    def __del__(self):
        self._connect.close()