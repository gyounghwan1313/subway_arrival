
import json
import datetime as dt
import os

from smart_open import open
import boto3


class S3Client(object):

    def __init__(self, aws_access_key: str, aws_secret_key: str):
        self.__aws_access_key = aws_access_key
        self.__aws_secret_key = aws_secret_key
        self.client = boto3.client("s3",
                                   aws_access_key_id=self.__aws_access_key,
                                   aws_secret_access_key=self.__aws_secret_key)

    def list_object(self, bucket: str, prefix: str):
        object_list = self.client.list_objects(Bucket=bucket, Prefix=prefix)
        object_list = [i for i in object_list['Contents']]
        return object_list

    def read_jsongz(self, s3_file_path: str):
        for json_line in open(s3_file_path, transport_params={"client": self.client}):
            json_file = json.loads(json_line)
        return json_file

    def get_latest_file(self, bucket: str, prefix: str):
        file_list = self.list_object(bucket, prefix)
        file_list.sort(key=lambda x: x['LastModified'] + dt.timedelta(hours=9))
        target_file = file_list[-1]
        file = self.read_jsongz(s3_file_path=f"s3://{bucket}/{target_file['Key']}")
        return file

    def upload_object(self, bucket: str, local_file_path: str, s3_file_path: str, delete: bool = False) -> None:
        """

        bucket: my-bucket
        local_file_path : ./data/test.csv
        s3_file_path : pre/fix/test.csv
        """
        self.client.upload_file(local_file_path, bucket, s3_file_path)
        if delete:
            os.remove(local_file_path)
            print(f"{local_file_path} was deleted")
