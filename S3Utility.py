# ------------------------------------------------------------------------
# Module Name :-   AWS S3 Utility
# Module description : - AWS S3 Functions
# Parameters required :-
# Created by :- Jatin Chauhan (jatin.chauhan@zs.com)
# Created on :- 21th Jan, 2019
# Version History :- v0.6
# ------------------------------------------------------------------------

import logging
import boto3
from botocore.exceptions import ClientError
from boto.s3.connection import S3Connection, Bucket, Key


class S3Utility:
    def __init__(self,
                 src_bucket_name: str,
                 src_access_key_id: str,
                 src_secret_key_id: str,
                 dst_bucket_name: str = False,
                 dst_access_key_id: str = False,
                 dst_secret_key_id: str = False):

        self.src_bucket_name = src_bucket_name
        self.src_access_key_id = src_access_key_id
        self.src_secret_key_id = src_secret_key_id
        self.src_s3 = boto3.client('s3', aws_access_key_id=src_access_key_id,
                                   aws_secret_access_key=src_secret_key_id)
        if dst_bucket_name and dst_access_key_id and dst_secret_key_id:
            self.dst_bucket_name = dst_bucket_name
            self.dst_access_key_id = dst_access_key_id
            self.dst_secret_key_id = dst_secret_key_id
            self.dst_s3 = boto3.client('s3', aws_access_key_id=dst_access_key_id,
                                       aws_secret_access_key=dst_secret_key_id)

    @staticmethod
    def copy_validator(src_s3, src_bucket_name: str, source_path: str,
                       dst_s3, dst_bucket_name: str, destination_path: str) -> bool:
        """
        A static helper function for the copy_s3_to_s3 definition
        :param src_s3: Source S3 Connection Object
        :param src_bucket_name: Source Bucket Name
        :param source_path: Source Path Name
        :param dst_s3: Destination S3 Connection Object
        :param dst_bucket_name: Destination Bucket Name
        :param destination_path: Destination Path Name
        :return: Boolean
        """
        try:
            for key in src_s3.list_objects(Bucket=src_bucket_name, Prefix=source_path)['Contents']:
                files = key['Key']
                destination_key = files.split('/')[-1]
                print(destination_key)
                source_response = src_s3.get_object(Bucket=src_bucket_name, Key=files)
                print(source_response['Body'])
                dst_s3.upload_fileobj(source_response['Body'], dst_bucket_name,
                                      destination_path + destination_key)

        except ClientError as e:
            logging.error(e)
            return False
        return True

    def copy_s3_to_s3(self, source_path: str, destination_path: str) -> None:
        """
        Copy contents of one S3 bucket to another S3 Bucket

        WARNING: Make sure you are creating the S3 Utility Object
            properly with Destination Bucket Credentials as well

            For Example:  S3Utility(src_bucket_name: str,
                                    src_access_key_id: str,
                                    src_secret_key_id: str,
                                    dst_bucket_name: str,
                                    dst_access_key_id: str,
                                    dst_secret_key_id: str)

        :param source_path: Source Path of the S3 Bucket 1
        :param destination_path: Destination Path of the S3 Bucket 2
        :return: None
        """
        if self.dst_bucket_name and self.dst_access_key_id and self.dst_secret_key_id:
            raise ValueError("""
                Destination S3 Object is not configured is not Configured
                Please use help(S3Utility) for more details
            """)
        logging.warning("""
            Transferring contents of S3
            (BUCKET) -> {self.src_bucket_name} PATH: {source_path}
            to
            (BUCKET) -> {self.dst_bucket_name} PATH: {destination_path}
        """.format(self=self, source_path=source_path, destination_path=destination_path))
        logging.basicConfig(level=logging.DEBUG, format='%(levelname)s: %(asctime)s: %(message)s')
        try:
            obj_status1 = self.src_s3.list_objects(Bucket=self.src_bucket_name,
                                                   Prefix=source_path)
            if obj_status1.get('Contents'):
                # Check if the files are copied successfully
                if self.copy_validator(self.src_s3, self.src_bucket_name, source_path,
                                       self.dst_s3, self.dst_bucket_name, destination_path):
                    logging.info('All the files are copied successfully')
                    self.src_s3.delete_object(Bucket=self.src_bucket_name, Key=source_path)
                    self.dst_s3.delete_object(Bucket=self.dst_bucket_name, Key=destination_path)
                    logging.warning('S3: Objects are successfully copied!')
                else:
                    self.src_s3.delete_object(Bucket=self.src_bucket_name, Key=source_path)
                    logging.warning('S3: There was an error with the above mentioned path')

        except ClientError as e:
            logging.error(e)

