
from typing import Union
import logging
import pandas as pd
import pandas.io.sql as psql


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

        import psycopg2 as pg
        self._connect = pg.connect(host=self.__host ,port=str(self.__port),database=self.__database,user=self.__user,password=self.__password )

    def sql_execute(self, query: str) -> None:
        self.cur = self._connect.cursor()
        try:
            self.cur.execute(query)
            # self.cur.commit()
            self._connect.commit()
            self.cur.close()
        except Exception as e:
            print(e)
            self.cur.close()

    def sql_dataframe(self, query: str) -> pd.DataFrame:
        df = psql.read_sql_query(query, self._connect)
        return df

    def sa_session(self):
        import sqlalchemy as sa
        self.sa_conn = sa.create_engine(f"postgresql://{self.__user}:{self.__password}@{self.__host}:{self.__port}/{self.__database}")

    def __del__(self):
        self._connect.close()


if __name__ == '__main__':
    __db_connector = PostgreSQL(host='ecsdfg.ap-northeast-2.compute.amazonaws.com',
                                port=5432,
                                database='asdf',
                                user='sdfg',
                                password='rsdfgsdfg')
