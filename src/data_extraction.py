from decouple import config
from sqlalchemy import inspect
from requests import get
from urllib.parse import urlparse
from pandas import DataFrame
import pandas as pd
import tabula
import boto3
import botocore


class DataExtractor():
    HEADERS = {"x-api-key": config("X-API-KEY")}
    NUMBER_OF_STORES_URL = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
    STORE_DATA_URL = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/%i"
    S3_ADDRESS = "s3://data-handling-public/products.csv"
    PDF_URL = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
    DATE_EVENTS_DATA_LINK="https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json"


    def __init__(self) -> None:
        pass

    def list_db_tables(self, engine) -> list:
        """
        List the tables in the database.

        Args:
            engine: SQLAlchemy database engine.

        Returns:
            list: List of table names.
        """
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        return tables

    def read_rds_table(self, table_name: str, engine) -> DataFrame:
        """
        Read table from an AWS RDS database.

        Args:
            table_name (str): Name of the table.
            engine: SQLAlchemy database engine.

        Returns:
            DataFrame: DataFrame containing the table data.
        """
        df = pd.read_sql_table(table_name, engine)
        return df

    def retrieve_pdf_data(self, url: str) -> DataFrame:
        """
        Retrieve table data from all pages in a PDF file.

        Args:
            url (str): URL of the PDF file.

        Returns:
            DataFrame: DataFrame containing the PDF data.
        """
        df = tabula.read_pdf(url, pages='all', force_subprocess=True)
        df = pd.concat(df)
        return df

    def list_number_of_stores(self, url, headers):
        """
        List the number of stores from a given URL.

        Args:
            url (str): URL to retrieve the number of stores.
            headers: Headers for the HTTP request.

        Returns:
            int: Number of stores.
        """
        res = get(url, headers=headers)
        data = res.json()
        number_of_stores = data["number_stores"]
        return number_of_stores

    def retrieve_store_data(self, url, headers, number_of_stores):
        """
        Retrieve store data from a given URL.

        Args:
            url (str): URL pattern for individual store data.
            headers: Headers for the HTTP request.
            number_of_stores (int): Number of stores.

        Returns:
            DataFrame: DataFrame containing store data.
        """
        stores_list = []
        for store_index in range(number_of_stores):
            store_url = url % (store_index)
            res = get(store_url, headers=headers)
            store_data = res.json()
            stores_list.append(store_data)
        stores_df = pd.DataFrame(stores_list)
        return stores_df

    def extract_from_s3(self, s3_address):
        """
        Extract data from an S3 bucket.

        Args:
            s3_address (str): S3 object address.

        Returns:
            DataFrame: DataFrame containing S3 data.
        """
        s3_address_data = self.parse_s3_address(s3_address)
        bucket_name = s3_address_data["BUCKET_NAME"]
        key = s3_address_data["KEY"]
        s3 = boto3.resource('s3')
        download_path = "./products.csv"
        try:
            s3.Bucket(bucket_name).download_file(key, download_path)
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                print("The s3 object does not exist.")
            else:
                raise
        products_df = pd.read_csv(download_path, index_col=0)
        return products_df

    def parse_s3_address(self, s3_address):
        """
        Parse an S3 object address.

        Args:
            s3_address (str): S3 object address.

        Returns:
            dict: Parsed S3 address data.
        """
        parsed_s3_address = urlparse(s3_address, allow_fragments=False)
        bucket_name = parsed_s3_address.netloc
        key = parsed_s3_address.path.lstrip("/")
        output_data = {
            "BUCKET_NAME": bucket_name,
            "KEY": key
        }
        return output_data

    def extract_date_events_data(self, file_address: str) -> pd.DataFrame:
        """
        Extract date events data from a file.

        Args:
            file_address (str): File address.

        Returns:
            DataFrame: DataFrame containing date events data.
        """
        date_events_df = pd.read_json(file_address)
        return date_events_df
