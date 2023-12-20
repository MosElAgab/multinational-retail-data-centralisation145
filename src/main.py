from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

# tools
print("connecting...")
connection = DatabaseConnector()
extractor = DataExtractor()
cleaning_util = DataCleaning()


# users data
print("processing users_data...")

# extract users data
engine = connection.init_db_engine()
table_name = "legacy_users"
users_df = extractor.read_rds_table(table_name, engine)

# clean users data
clean_users_df = cleaning_util.clean_user_data(users_df)

# upload users data
table_name = "dim_users"
upload_engine = connection.init_upload_db_engine()
connection.upload_to_db(clean_users_df, table_name, upload_engine)


# cards data
print("processing cards_data...")
# extract card data
url = extractor.pdf_url
cards_df = extractor.retrieve_pdf_data(url)

# clean card data
clean_card_df = cleaning_util.clean_card_data(cards_df)

# upload card data
table_name = "dim_card_details"
upload_engine = connection.init_upload_db_engine()
connection.upload_to_db(clean_card_df, table_name, upload_engine)

# clean_users_df.info()
print("end")