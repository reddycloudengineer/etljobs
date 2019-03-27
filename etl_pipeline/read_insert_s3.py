import sqlite3
import os
import boto3

AWS_ACCESS_KEY = os.environ('ACCESS_KEY')
AWS_SECRET_KEY = os.environ('SECRET_ACCESS_KEY')

session = boto3.Session(
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
)


class S3DataProcessing:

    def __init__(self, bucket_name):
        """
        This will create a s3 resource connection using boto3
        :param bucket_name:
        """

        self.s3_resource = boto3.resource('s3')
        self.bucket_name = bucket_name

    def read_all_files_bucket(self, country_codes=[]):
        """
        This will read all the files from the s3 bucket.
        :param country_codes:
        :return: key/ file name
        """
        bucket = self.s3_resource.Bucket(self.bucket_name)
        for obj in bucket.objects.all():
            key = obj.key
            body = obj.get()['Body'].read()
            bucket.download_file(obj.key, key)
            yield key

    def process_data_bucket(self, country_codes=[], query=None):
        """
        processing the data one file at a time and inserting and writing back to S3 bucket
        :param country_codes:
        :param query:
        :return:
        """
        files = self.read_all_files_bucket(country_codes)
        for file in files:
            # print(file,country_codes)
            if any(s.lower() in file.lower() for s in country_codes):
                self.insert_record(file, query)
                self.write_to_s3(file)

    def insert_record(self, file_name, insert_query):
        """
        Inserting the data with the help of insert query
        :param file_name:
        :param insert_query:
        :return:
        """
        try:
            sqlite_file = file_name
            conn = sqlite3.connect(sqlite_file)
            cursor = conn.cursor()
            cursor.execute(insert_query)
            conn.commit()
        except Exception as e:
            print(e)

    def write_to_s3(self, file_name):
        """
        Write the file which is downloaded back. If in case you want to store the files
        Please remove the os.remove(file_name). This will store all teh files in local disk.
        :param file_name:
        :return:
        """
        bucket = self.s3_resource.Bucket(self.bucket_name)
        bucket.upload_file(file_name, file_name)
        os.remove(file_name)


bucket_name = 'Enter your bucket name here'
dataprocessing = S3DataProcessing(bucket_name)
country_codes = ['US']
insert_query = '''
Enter your insert query here 
'''
dataprocessing.process_data_bucket(country_codes, insert_query)
