import boto3
import pandas as pd
import json
import fastavro

class S3Browser:
    def __init__(self, bucket_name):
        self.s3 = boto3.client('s3')
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
                if selected_file.endswith('.csv') or selected_file.endswith('.parquet') or selected_file.endswith('.json') or selected_file.endswith('.txt') or selected_file.endswith('.avro'):
                    return selected_file

    def read_dataframe(self, file_key):
        obj = self.s3.get_object(Bucket=self.bucket_name, Key=file_key)
        ext = file_key.split('.')[-1]
        if ext == 'csv':
            df = pd.read_csv(obj['Body'])
        elif ext == 'parquet':
            df = pd.read_parquet(obj['Body'])
        elif ext == 'json':
            df = pd.read_json(obj['Body'])
        elif ext == 'txt':
            df = pd.read_csv(obj['Body'], sep='\t')
        elif ext == 'avro':
            avro_schema = fastavro.schema.loads(obj['Body'].read())
            avro_records = list(fastavro.reader(obj['Body'], avro_schema))
            df = pd.DataFrame.from_records(avro_records)
        return df

if __name__ == "__main__":
    bucket_name = input("Enter the name of the S3 bucket: ")
    s3_browser = S3Browser(bucket_name)
    while True:
        selected_file = s3_browser.navigate()
        dataframe = s3_browser.read_dataframe(selected_file)
        print("\nDataFrame Preview:")
        print(dataframe.head())
        
        choice = input("\nDo you want to continue browsing and opening files? (yes/no): ")
        if choice.lower() != 'yes':
            break


"""import boto3
import pandas as pd
import json

class S3Browser:
    def __init__(self, bucket_name):
        self.s3 = boto3.client('s3')
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
                if selected_file.endswith('.csv') or selected_file.endswith('.parquet') or selected_file.endswith('.json'):
                    return selected_file

    def read_dataframe(self, file_key):
        obj = self.s3.get_object(Bucket=self.bucket_name, Key=file_key)
        ext = file_key.split('.')[-1]
        if ext == 'csv':
            df = pd.read_csv(obj['Body'])
        elif ext == 'parquet':
            df = pd.read_parquet(obj['Body'])
        elif ext == 'json':
            df = pd.read_json(obj['Body'])
        return df

if __name__ == "__main__":
    bucket_name = 'ayra-v2-emea-stage'  # Substitua pelo nome do seu bucket S3
    s3_browser = S3Browser(bucket_name)
    while True:
        selected_file = s3_browser.navigate()
        dataframe = s3_browser.read_dataframe(selected_file)
        print("\nDataFrame Preview:")
        print(dataframe.head())
        
        choice = input("\nDo you want to continue browsing and opening files? (yes/no): ")
        if choice.lower() != 'yes':
            break
"""