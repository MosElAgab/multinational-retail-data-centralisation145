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

headers = {
    "x-api-key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"
}
url = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/1"
res = get(url, headers=headers)
data = res.json()
print(data)



# extractor = DataExtractor()
# x = extractor.list_number_of_stores()
# print(x)
# summary
# duplicate gives NULL
# card numbers contain ?
# check expiry date adhere to format
# remove code like entries
# validate date format
