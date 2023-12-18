from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

# tools
connection = DatabaseConnector()
extractor = DataExtractor()
cleaning_util = DataCleaning()

# extract users data
engine = connection.init_db_engine()
table_name = "legacy_users"
users_df = extractor.read_rds_table(table_name, engine)

# clean users data
cleaned_users_df = cleaning_util.clean_user_data(users_df)

# upload users data
table_name = "dim_users"
upload_engine = connection.init_upload_db_engine()
connection.upload_to_db(cleaned_users_df, table_name, upload_engine)
