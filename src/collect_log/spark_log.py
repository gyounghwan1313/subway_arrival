
import os
import sys
import re
import datetime as dt
import traceback

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from src.module.logging_util import LoadLogger
from src.module.db_connection import PostgreSQL


class SparkLogCollect(object):

    def __init__(self,
                 db_conn: PostgreSQL,
                 file_path: str):
        self.db_conn = db_conn
        self.file_path = file_path
        self._read_log()

    def _read_log(self) -> None:
        with open(self.file_path, "r") as file:
            self.log_file = file.readlines()

    def _find_message(self) -> None:
        self.message = [i for i in self.log_file if "Raw Data Count :" in i][-1]

    def _parsing_date(self) -> None:
        self.log_date_str = re.findall(pattern="\w{4}-\w{2}-\w{2}", string=self.message)[-1]
        self.log_datetime = dt.datetime.strptime(self.log_date_str, "%Y-%m-%d")

    def _parsing_count(self) -> None:
        self.data_count = re.findall(pattern="\d+\\n", string=self.message)[-1]
        self.data_count = int(self.data_count.replace("\n", ""))

    def parsing_message(self) -> None:
        self._find_message()
        self._parsing_date()
        self._parsing_count()

    def save(self) -> None:
        self.db_conn.sql_execute(f"""INSERT INTO public.aggr_collected_raw_data ("date", cnt, created_time, updated_time) VALUES('{self.log_datetime-dt.timedelta(days=1)}', {self.data_count}, now(), now());""")


if __name__ == '__main__':

    db_host = os.environ['DB_HOST']
    db_port = os.environ['DB_PORT']
    db_database = os.environ['DB_DATABASE']
    db_user = os.environ['DB_USER']
    db_password = os.environ['DB_PASSWORD']

    logger_class = LoadLogger()
    # logger = logger_class.stream("Test")
    logger = logger_class.time_rotate_file(log_dir="/log/", file_name="collect_spark_log.log")
    logger.info(
        f"""===========ENV===========\n DB : {db_host} / {db_port} / {db_database} / {db_user} / {db_password} \n ========================="""
    )

    try:
        db_conn = PostgreSQL(host=db_host,
                             port=db_port,
                             database=db_database,
                             user=db_user,
                             password=db_password)

        log_collect = SparkLogCollect(db_conn=db_conn, file_path="/log/spark_aggr.log")
        log_collect.parsing_message()
        log_collect.save()

    except Exception as e:
        logger.error(e)
        logger.error(traceback.format_exc())
        for k, v in log_collect.__dict__.items():
            logger.error("== timetable Class ==")
            logger.error("Key : ", k)
            logger.error("Value : ", v)
        sys.exit(1)





