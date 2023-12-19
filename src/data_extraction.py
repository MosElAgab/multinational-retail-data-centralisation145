import pandas as pd
import tabula
from sqlalchemy import inspect


class DataExtractor():
    def __init__(self) -> None:
        # self.pdf_url = """https://data-handling-public.s3.eu-west
        # -1.amazonaws.com/card_details.pdf"""
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
