from dateutil.parser import parse, ParserError
import pandas as pd, numpy as np
from datetime import datetime as dt
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
file_path = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
# # # file_path = './card_details.pdf'
data_extractor = DataExtractor()
url = data_extractor.pdf_url
df = data_extractor.retrieve_pdf_data(url)


# clean
cleaning_util = DataCleaning()
clean_df = cleaning_util.clean_card_data(df)
def is_valid(date):
    try:
        string_date = str(date)
        dt.strptime(string_date, "%Y-%m-%d")
        # string_date = parse(string_date, dayfirst=True
        return True
    except ParserError:
        print('parser error', date)
        return False
    except TypeError:
        print("type error", date)
        return False
    except ValueError:
        print("value error", date)
        return False
mask = clean_df["date_payment_confirmed"].apply(is_valid)
print(clean_df[mask])

clean_df.info()






# summary
# duplicate gives NULL
# card numbers contain ?
# check expiry date adhere to format
# remove code like entries
# validate date format
