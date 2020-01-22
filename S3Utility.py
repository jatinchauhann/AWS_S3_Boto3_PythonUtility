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

        logging.warning("S3 Utility")
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
        else:
            logging.warning("S3: Second S3 Bucket is not Configured!")

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
        :return: bool
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

    def copy_s3_to_s3(self, source_path: str, destination_path: str) -> bool:
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
        :return: bool
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
                    return True
                else:
                    self.src_s3.delete_object(Bucket=self.src_bucket_name, Key=source_path)
                    logging.warning('S3: There was an error with the above mentioned path')
                    return False
            return False
        except ClientError as e:
            logging.error(e)

    def copy_local_to_s3(self, local_path_to_file: str, s3_path: str) -> bool:
        """
        Copy contents of Local File System to S3 Path
        :param local_path_to_file: Fully Qualified path to the local file which you wish to move to S3
        :param s3_path: S3 path where you wish to move your file
        :return:
        """
        try:
            self.src_s3.upload_file(local_path_to_file,
                                    self.src_bucket_name,
                                    s3_path)
            return True
        except ClientError as e:
            logging.error(e)
            return False

    def list_objects_in_s3(self, s3_path: str) -> list:
        """
        Retrieves list of Objects in the S3 Path specified
        :param s3_path: S3 Path from where you wish to retrieve the list of objects
        :return: list
        """
        object_list = []
        try:
            for key in self.src_s3.list_objects(Bucket=self.src_bucket_name, Prefix=s3_path)['Contents']:
                object_list.append(key['Key'])
            object_list.pop(0)
            return object_list
        except ClientError as e:
            logging.error(e)
            return object_list

    def delete_directory_from_s3(self, s3_path: str) -> bool:
        """
        Delete all the keys in the S3 Path
        :param s3_path: S3 Path from where you wish to delete all the sub-keys
        :return: bool
        """
        try:
            logging.warning("S3: Deleting (BUCKET) -> {self.src_bucket_name} : {s3_path}".format(self=self,
                                                                                                 s3_path=s3_path))
            for key in self.src_s3.list_objects(Bucket=self.src_bucket_name, Prefix=s3_path)['Contents']:
                key.delete()
            return True
        except ClientError as e:
            logging.error(e)
            return False
