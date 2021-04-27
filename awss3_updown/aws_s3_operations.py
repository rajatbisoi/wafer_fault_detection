import boto3
import uuid
import pandas as pd
import os
from application_logging.logger import App_Logger


class Aws_Bucket_operation:
    def __init__(self,local_file_name_address='config/bucket_name'):
        self.s3_resource = boto3.resource('s3')
        self.bucket_name = None
        self.logger = App_Logger('wafer')
        self.local_file_name_address=local_file_name_address

    def Create_Bucket_Name(self, bucket_prefix):
        # The generated bucket name must be between 3 and 63 chars long
        temp = str(uuid.uuid4()).lower()
        return ''.join([bucket_prefix, temp])

    def Create_Bucket(self, bucket_prefix='wafer-training'):
        session = boto3.session.Session()
        current_region = session.region_name
        bucket_name = self.Create_Bucket_Name(bucket_prefix)
        bucket_response = self.s3_resource.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={
                'LocationConstraint': current_region})
        # print(bucket_name, current_region)
        self.bucket_name = bucket_name
        self.logger.log('wafer_log', 's3 bucket created:!'.format(bucket_name))
        open(self.local_file_name_address, 'a').write("\n" + bucket_name)

    def Upload_To_S3(self, path, file_name,bucket_prefix='wafer-training'):
        if self.bucket_name == None:
            with open(self.local_file_name_address, 'r') as name_store:
                for line in name_store:
                    if bucket_prefix in line:
                        self.bucket_name = str(line).rstrip("\n")
        self.s3_resource.Object(self.bucket_name, file_name).upload_file(path)
        self.logger.log('wafer_log', 'file {0} uploaded to S3!'.format(file_name))

    def Download_From_S3(self, file_name,bucket_prefix='wafer-training'):
        if self.bucket_name == None:
            with open(self.local_file_name_address, 'r') as name_store:
                for line in name_store:
                    if bucket_prefix in line:
                        self.bucket_name = str(line).rstrip("\n")
        content = self.s3_resource.Object(self.bucket_name, file_name)
        content.get()['Body'].read().decode('utf-8')
        self.logger.log('wafer_log', 'file {0} downloaded from s3!'.format(file_name))
        return pd.read_csv(content.get()['Body'])

    def Upload_Everything_To_S3(self,folder_in_local='Training_Batch_Files',bucket_prefix='wafer-training'):
        if self.bucket_name == None:
            with open(self.local_file_name_address, 'r') as name_store:
                for line in name_store:
                    if bucket_prefix in line:
                        self.bucket_name = str(line).rstrip("\n")
        for file_name in os.listdir(folder_in_local+'/'):
            self.s3_resource.Object(self.bucket_name, file_name).upload_file(folder_in_local+'/' + file_name)
            self.logger.log('wafer_log', 'file {0} uploaded to S3!'.format(file_name))

    def Create_S3_Bucket_Instance(self,bucket_prefix='wafer-training'):
        if self.bucket_name == None:
            with open(self.local_file_name_address, 'r') as name_store:
                for line in name_store:
                    if bucket_prefix in line:
                        self.bucket_name = str(line).rstrip("\n")
        return self.s3_resource.Bucket(name=self.bucket_name)

    def Upload_To_S3_obj(self, object, file_name, bucket_prefix='wafer-training'):
        try:
            if self.bucket_name == None:
                with open(self.local_file_name_address, 'r') as name_store:
                    for line in name_store:
                        if bucket_prefix in line:
                            self.bucket_name = str(line).rstrip("\n")
            self.s3_resource.Object(self.bucket_name, file_name).put(Body=object)
            self.logger.log('wafer_log', 'object {0} uploaded to S3!'.format(file_name))
        except Exception as err:
            print(str(err))

    def Download_From_S3_raw(self, file_name, bucket_prefix='wafer-training'):
        if self.bucket_name == None:
            with open(self.local_file_name_address, 'r') as name_store:
                for line in name_store:
                    if bucket_prefix in line:
                        self.bucket_name = str(line).rstrip("\n")
        content = self.s3_resource.Object(self.bucket_name, file_name)
        content.get()['Body'].read()
        self.logger.log('wafer_log', 'raw file {0} downloaded from s3!'.format(file_name))
        return content.get()['Body'].read()

    def Delete_From_S3(self, file_name, bucket_prefix='wafer-training'):
        if self.bucket_name == None:
            with open(self.local_file_name_address, 'r') as name_store:
                for line in name_store:
                    if bucket_prefix in line:
                        self.bucket_name = str(line).rstrip("\n")
        self.s3_resource.Object(self.bucket_name, file_name).delete()

