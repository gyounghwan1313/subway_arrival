
from typing import Union
import logging
import pandas as pd
import pandas.io.sql as psql
import psycopg2 as pg


class PostgreSQL(object):

    def __init__(self,
                 host,
                 port: Union[str, int],
                 database: str,
                 user: str,
                 password: str):

        self.__host = host
        self.__port = port
        self.__database = database
        self.__user = user
        self.__password = password

    def sql_execute(self, query: str) -> None:
        self._connect = pg.connect(host=self.__host, port=str(self.__port), database=self.__database, user=self.__user,
                                   password=self.__password)
        self.cur = self._connect.cursor()
        try:
            self.cur.execute(query)
            self._connect.commit()
            self.cur.close()
            self._connect.close()

        except Exception as e:
            print(e)
            self.cur.close()
            self._connect.close()

    def sql_dataframe(self, query: str) -> pd.DataFrame:
        self._connect = pg.connect(host=self.__host, port=str(self.__port), database=self.__database, user=self.__user,
                                   password=self.__password)
        df = psql.read_sql_query(query, self._connect)
        self._connect.close()
        return df

    def sa_session(self):
        import sqlalchemy as sa
        self.sa_conn = sa.create_engine(f"postgresql://{self.__user}:{self.__password}@{self.__host}:{self.__port}/{self.__database}")


if __name__ == '__main__':
    __db_connector = PostgreSQL(host='ecsdfg.ap-northeast-2.compute.amazonaws.com',
                                port=5432,
                                database='asdf',
                                user='sdfg',
                                password='rsdfgsdfg')
