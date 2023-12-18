import pandas as pd
from sqlalchemy import inspect


class DataExtractor():
    def __init__(self) -> None:
        pass

    def list_db_tables(self, engine):
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        return tables

    def read_rds_table(self, table_name, engine):
        df = pd.read_sql_table(table_name, engine)
        return df
