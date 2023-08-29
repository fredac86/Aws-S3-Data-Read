import boto3
import pandas as pd
import json
import fastavro
from io import BytesIO

class S3Browser:
    def __init__(self, access_key, secret_key):
        self.s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
        self.bucket_name = None

    def set_bucket(self, bucket_name):
        self.bucket_name = bucket_name

    def list_folders(self, prefix=''):
        response = self.s3.list_objects_v2(Bucket=self.bucket_name, Prefix=prefix, Delimiter='/')
        folders = [common_prefix['Prefix'] for common_prefix in response.get('CommonPrefixes', [])]
        return folders

    def list_files(self, prefix=''):
        response = self.s3.list_objects_v2(Bucket=self.bucket_name, Prefix=prefix, Delimiter='/')
        files = [content['Key'] for content in response.get('Contents', [])]
        return files

    def navigate(self):
        current_prefix = ''
        while True:
            print("\nCurrent Folder:", current_prefix)
            folders = self.list_folders(prefix=current_prefix)
            files = self.list_files(prefix=current_prefix)
            
            print("\nFolders:")
            for index, folder in enumerate(folders):
                print(f"{index + 1}. {folder}")
            
            print("\nFiles:")
            for index, file in enumerate(files):
                print(f"{index + 1 + len(folders)}. {file}")
            
            choice = int(input("\nEnter folder or file number to navigate (0 to go back): "))
            
            if choice == 0 and current_prefix:
                current_prefix = '/'.join(current_prefix.split('/')[:-2]) + '/'
            elif 1 <= choice <= len(folders):
                current_prefix = folders[choice - 1]
            elif len(folders) < choice <= len(folders) + len(files):
                selected_file = files[choice - 1 - len(folders)]
                if selected_file.endswith('.csv') or selected_file.endswith('.parquet') or selected_file.endswith('.json') or selected_file.endswith('.avro'):
                    return selected_file

    def read_dataframe(self, file_key):
        obj = self.s3.get_object(Bucket=self.bucket_name, Key=file_key)
        ext = file_key.split('.')[-1]
        if ext == 'csv':
            df = pd.read_csv(obj['Body'])
        elif ext == 'parquet':
            with BytesIO(obj['Body'].read()) as buffer:
                df = pd.read_parquet(buffer)
        elif ext == 'json':
            df = pd.read_json(obj['Body'])
        elif ext == 'avro':
            avro_schema = fastavro.schema.loads(obj['Body'].read())
            avro_records = list(fastavro.reader(obj['Body'], avro_schema))
            df = pd.DataFrame.from_records(avro_records)
        
        return df

if __name__ == "__main__":
    access_key = input("Enter your AWS access key: ")
    secret_key = input("Enter your AWS secret key: ")
    
    s3_browser = S3Browser(access_key, secret_key)
    while True:
        bucket_name = input("\nEnter the name of the S3 bucket: ")
        s3_browser.set_bucket(bucket_name)
        
        selected_file = s3_browser.navigate()
        dataframe = s3_browser.read_dataframe(selected_file)
        print("\nDataFrame Preview:")
        print(dataframe.head())
        
        choice = input("\nDo you want to continue browsing and opening files? (yes/no): ")
        if choice.lower() != 'yes':
            break
