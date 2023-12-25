import pandas as pd
import numpy as np
from src.database_utils import DatabaseConnector
from src.data_extraction import DataExtractor
from src.data_cleaning import DataCleaning

# loading data
connection = DatabaseConnector()
engine = connection.init_db_engine()
extractor = DataExtractor()
cleaning_util = DataCleaning()
table_name = "legacy_users"
users_df = extractor.read_rds_table(table_name, engine)

# To download store data to local drive as csv file:
# run
# extractor = DataExtractor()
# url = extractor.NUMBER_OF_STORES_URL
# headers = extractor.HEADERS
# number_of_stores = extractor.list_number_of_stores(url, headers)
# url = extractor.STORE_DATA_URL
# stores_df = extractor.retrieve_store_data(url, headers, number_of_stores)
# stores_df.to_csv("stores_data.csv", index=False)