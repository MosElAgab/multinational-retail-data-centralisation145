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
print("...extracting")
engine = connection.init_db_engine()
table_name = "legacy_users"
users_df = extractor.read_rds_table(table_name, engine)

# clean users data
print("...cleaning")
clean_users_df = cleaning_util.clean_user_data(users_df)

# upload users data
print("...uploading")
table_name = "dim_users"
upload_engine = connection.init_upload_db_engine()
connection.upload_to_db(clean_users_df, table_name, upload_engine)


# cards data
print("processing cards_data...")

# extract card data
print("...extracting")
url = extractor.pdf_url
cards_df = extractor.retrieve_pdf_data(url)

# clean card data
print("...cleaning")
clean_card_df = cleaning_util.clean_card_data(cards_df)

# upload card data
print("...uploading")
table_name = "dim_card_details"
upload_engine = connection.init_upload_db_engine()
connection.upload_to_db(clean_card_df, table_name, upload_engine)


# stores data
print("processing cards_data...")

# extract stores data
print("...extracting")
url = extractor.NUMBER_OF_STORES_URL
headers = extractor.HEADERS
number_of_stores = extractor.list_number_of_stores(url, headers)
url = extractor.STORE_DATA_URL
stores_df = extractor.retrieve_store_data(url, headers, number_of_stores)

# clean stores data
print("...cleaning")
clean_stores_df = cleaning_util.clean_store_data(stores_df)

# upload store data
print("...uploading")
table_name = "dim_store_details"
upload_engine = connection.init_upload_db_engine()
connection.upload_to_db(clean_stores_df, table_name, upload_engine)


# end

# clean_users_df.info()
print("end")