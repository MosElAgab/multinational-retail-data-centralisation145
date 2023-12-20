from dateutil.parser import parse, ParserError
import pandas as pd, numpy as np
from datetime import datetime as dt
from requests import get
# import numpy as np
from sqlalchemy import create_engine, engine
from src.database_utils import DatabaseConnector
from src.data_extraction import DataExtractor
from src.data_cleaning import DataCleaning
import re
import tabula
import os

# os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk/libexec/openjdk.jdk"
# tabula.environment_info()

# extract
# extractor = DataExtractor()
# url = extractor.NUMBER_OF_STORES_URL
# headers = extractor.HEADERS
# number_of_stores = extractor.list_number_of_stores(url, headers)
# url = extractor.STORE_DATA_URL
# stores_df = extractor.retrieve_store_data(url, headers, number_of_stores)
# print(stores_df)

# stores_df.to_csv("stores_data.csv")


# extract
stores_df = pd.read_csv("stores_data.csv")
print(stores_df)