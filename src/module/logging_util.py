
import sys
from collections.abc import Callable
from typing import Optional
import logging
import logging.handlers as log_handler


class LoadLogger(object):

    def __init__(self,
                 logging_format: str = '%(levelname)-8s %(asctime)s pid:%(process)d [%(filename)s:%(lineno)d] [%(module)s:%(funcName)s] >> %(message)s',
                 date_format: str = '%Y-%m-%d %H:%M:%S',
                 level: Callable[int] = logging.INFO):
        self.logging_format = logging_format
        self.date_format = date_format
        self.level = level

    def stream(self, name: str = __name__):

        if "logger" in self.__dict__:
            print("logger already created")
            sys.exit(1)
        else:
            self.logger = logging.getLogger(name)
            stream_handler = logging.StreamHandler()
            logging.basicConfig(format=self.logging_format,
                                datefmt=self.date_format,
                                level=self.level,
                                handlers=[stream_handler])

            return self.logger

    def time_rotate_file(self,
                         log_dir: str,
                          file_name: str,
                          log_suffix: str = '-%Y-%m-%d',
                          change_time: Optional[str] = 'midnight',
                          interval_day: int = 1,
                          retention_count: int = 30,
                          at_time: Optional[str] = None,
                          name: str = __name__):

        if "logger" in self.__dict__:
            print("logger already created")
            sys.exit(1)
        else:
            self.logger = logging.getLogger(name)
            time_file_handler = log_handler.TimedRotatingFileHandler(filename=log_dir + file_name,
                                                                     when=change_time,
                                                                     interval=interval_day,
                                                                     backupCount=retention_count,
                                                                     atTime=at_time,
                                                                     encoding='utf-8')
            time_file_handler.suffix = log_suffix
            logging.basicConfig(format=self.logging_format,
                                datefmt=self.date_format,
                                level=self.level,
                                handlers=[time_file_handler])

            return self.logger


if __name__ == '__main__':
    # test = LoadLogger()
    # a = test.stream()
    # a.info("ttt")
    test2 = LoadLogger()
    b = test2.time_rotate_file(log_dir="./",file_name="test2")
    b.info("tsdvtt")





