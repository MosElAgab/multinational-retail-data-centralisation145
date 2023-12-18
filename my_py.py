from dateutil.parser import parse, ParserError
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from src.database_utils import DatabaseConnector
from src.data_extraction import DataExtractor
from src.data_cleaning import DataCleaning
import re

# extraction
connection = DatabaseConnector()
engine = connection.init_db_engine()
extractor = DataExtractor()
cleaning_util = DataCleaning()
table_name = "legacy_users"
users_df = extractor.read_rds_table(table_name, engine)

# cleaing
cleaned_users_df = cleaning_util.clean_user_data(users_df)
# cleaned_users_df.set_index("index", inplace=True)


# loading
table_name = "dim_users"
upload_engine = connection.init_upload_db_engine()
connection.upload_to_db(cleaned_users_df, table_name, upload_engine)