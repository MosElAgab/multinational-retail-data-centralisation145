import pandas as pd
import tabula
import boto3
import botocore
from decouple import config
from sqlalchemy import inspect
from requests import get
from urllib.parse import urlparse


class DataExtractor():
    NUMBER_OF_STORES_URL = config("NUMBER_OF_STORES_URL")
    HEADERS = {
        "x-api-key": config("X-API-KEY")
    }
    STORE_DATA_URL = config("STORE_DATA_URL")
    S3_ADDRESS = config("S3_ADDRESS")
    def __init__(self) -> None:
        self.pdf_url = config("PDF_URL")
        # self.pdf_url = "./card_details.pdf"

    def list_db_tables(self, engine):
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        return tables

    def read_rds_table(self, table_name, engine):
        df = pd.read_sql_table(table_name, engine)
        return df

    def retrieve_pdf_data(self, url: str) -> pd.DataFrame:
        df = tabula.read_pdf(url, pages='all', force_subprocess=True)
        df = pd.concat(df)
        return df

    def list_number_of_stores(self, url, headers):
        res = get(url, headers=headers)
        data = res.json()
        number_of_stores = data["number_stores"]
        return number_of_stores

    def retrieve_store_data(self, url, headers, number_of_stores):
        stores_list = []
        for store_index in range(number_of_stores):
            store_url = url % (store_index)
            res = get(store_url, headers=headers)
            store_data = res.json()
            stores_list.append(store_data)
        stores_df = pd.DataFrame(stores_list)
        return stores_df

    def extract_from_s3(self, s3_address):
        s3_address_data = self.parse_s3_address(s3_address)
        bucket_name = s3_address_data["BUCKET_NAME"]
        key = s3_address_data["KEY"]
        s3 = boto3.resource('s3')
        products_download_file_path = config("PRODUCTS_DOWNLOAD_FILE_PATH")
        try:
            s3.Bucket(bucket_name).download_file(key, products_download_file_path)
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                print("The s3 object does not exist.")
            else:
                raise
        products_df = pd.read_csv(products_download_file_path, index_col=0)
        return products_df

    def parse_s3_address(self, s3_address):
        parsed_s3_address =  urlparse(s3_address, allow_fragments=False)
        bucket_name = parsed_s3_address.netloc
        key = parsed_s3_address.path.lstrip("/")
        output_data = {
            "BUCKET_NAME": bucket_name,
            "KEY": key
        }
        return output_data
    
    def extract_date_events_data(self, file_address: str) -> pd.DataFrame:
        date_events_df = pd.read_json(file_address)
        return date_events_df
