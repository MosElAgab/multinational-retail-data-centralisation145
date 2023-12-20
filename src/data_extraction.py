import pandas as pd
import tabula
from sqlalchemy import inspect
from requests import get

class DataExtractor():
    NUMBER_OF_STORES_URL = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
    HEADERS = {
    "x-api-key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"
    }

    def __init__(self) -> None:
        # self.pdf_url = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
        self.pdf_url = "./card_details.pdf"

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

    def list_number_of_stores(self):
        url = self.NUMBER_OF_STORES_URL
        headers = self.HEADERS
        res = get(url, headers=headers)
        data = res.json()
        number_of_stores = data["number_stores"]
        return number_of_stores
