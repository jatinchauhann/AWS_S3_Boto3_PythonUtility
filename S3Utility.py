# Module Name :-   AWS S3 Utility
# Module description : - AWS S3 Functions using BOTO3
# Created by :- Jatin Chauhan (jatin.chauhan@zs.com)
# Created on :- 21th Jan, 2019
# Version History :- v0.6

# ##################### AWS BOTO3 License ###############################

# Copyright 2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You
# may not use this file except in compliance with the License. A copy of
# the License is located at
#
# http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is
# distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
# ANY KIND, either express or implied. See the License for the specific
# language governing permissions and limitations under the License.
# ##################### AWS BOTO3 License ###############################

import logging
import boto3
from botocore.exceptions import ClientError


class S3Utility:
    def __init__(self,
                 src_bucket_name: str,
                 src_access_key_id: str,
                 src_secret_key_id: str,
                 dst_bucket_name: str = None,
                 dst_access_key_id: str = None,
                 dst_secret_key_id: str = None):
        """
        S3 Utility build using AWS's BOTO3 Library for easy python
        integration which uses AWS S3 Storage class

        :param src_bucket_name: Source S3 Configuration Parameter: S3 Bucket Name
        :param src_access_key_id: Source S3 Configuration Parameter: S3 Access Key
        :param src_secret_key_id: Source S3 Configuration Parameter: S3 Secret Key
        :param dst_bucket_name: Destination S3 Configuration Parameter: S3 Bucket Name
        :param dst_access_key_id: Destination S3 Configuration Parameter: S3 Access Key
        :param dst_secret_key_id: Destination S3 Configuration Parameter: S3 Secret Key
        """
        logging.warning("S3 Utility")
        self.src_bucket_name = src_bucket_name
        self.src_access_key_id = src_access_key_id
        self.src_secret_key_id = src_secret_key_id
        self.src_s3 = boto3.client('s3', aws_access_key_id=src_access_key_id,
                                   aws_secret_access_key=src_secret_key_id)
        if dst_bucket_name is not None \
                and dst_access_key_id is not None and dst_secret_key_id is not None:
            self.dst_bucket_name = dst_bucket_name
            self.dst_access_key_id = dst_access_key_id
            self.dst_secret_key_id = dst_secret_key_id
            self.dst_s3 = boto3.client('s3', aws_access_key_id=dst_access_key_id,
                                       aws_secret_access_key=dst_secret_key_id)
        else:
            logging.warning("""S3: Second S3 Bucket is not Configured! \n\t 
                                Required if you are using copy_s3_to_s3 function""")

        logging.warning("S3: Successfully Configured S3 Object")

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

        WARNING: If you specify fully qualified path in the <source_path> of individual files, they will be MOVED and
                not copied. If you specify <source_path> till the folder name, all of the files in that folder will
                be recurrently copied!

        :param source_path: Source Path of the S3 Bucket 1
        :param destination_path: Destination Path of the S3 Bucket 2
        :return: bool
        """
        if self.dst_bucket_name is None \
                and self.dst_access_key_id is None and self.dst_secret_key_id is None:
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
            logging.warning("""
                Copied
                from
                LOCAL PATH: {local_path_to_file}
                to
                (BUCKET) -> {self.src_bucket_name} : {s3_path}
            """.format(self=self, s3_path=s3_path, local_path_to_file=local_path_to_file))
            return True
        except ClientError as e:
            logging.error(e)
            return False

    def copy_s3_to_local(self, s3_path, file_name, local_path_to_file):
        """
        opy contents of S3 to Local File System Path
        :param s3_path: S3 path from where you wish to copy your file. This should be exclusive of the file name
        :param file_name: File name that you wish to copy
        :param local_path_to_file: Fully Qualified path to the local file where you wish to copy your S3 Contents
        :return:
        """
        try:
            self.src_s3.download_file(self.src_bucket_name, (s3_path + file_name), (local_path_to_file + file_name))
            logging.warning("""
                Copied {file_name}
                from
                (BUCKET) -> {self.src_bucket_name} : {s3_path}
                to
                LOCAL PATH: {local_path_to_file}
            """.format(self=self, s3_path=s3_path, local_path_to_file=local_path_to_file, file_name=file_name))
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
            logging.warning("""
                List of Keys
                (BUCKET) -> {self.src_bucket_name} : {s3_path}
            """.format(self=self, s3_path=s3_path))
            return object_list
        except ClientError as e:
            logging.error(e)
            return object_list

    def delete_from_s3(self, s3_path, file_name="") -> bool:
        """
        Delete Keys from S3 Bucket under the s3_path
        :param s3_path: S3 Path from where you want to delete all the sub-keys
        :param file_name: File name that you want to delete
        :return:
        """
        try:
            self.src_s3.delete_object(Bucket=self.src_bucket_name, Key=(s3_path + file_name))
            logging.warning("""
                Deleted directory
                (BUCKET) -> {self.src_bucket_name} : {s3_path}
            """.format(self=self, s3_path=s3_path))
            return True
        except ClientError as e:
            logging.error(e)
            return False
