from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning
from decouple import config

# tools
print("connecting...")
connection = DatabaseConnector()
extractor = DataExtractor()
cleaning_util = DataCleaning()


# *** users data
print("*** processing users_data")

# extract users data
print("...extracting")
extract_engine = connection.init_db_engine()
table_name = "legacy_users"
users_df = extractor.read_rds_table(table_name, extract_engine)

# clean users data
print("...cleaning")
clean_users_df = cleaning_util.clean_user_data(users_df)

# upload users data
print("...uploading")
table_name = "dim_users"
upload_engine = connection.init_upload_db_engine()
connection.upload_to_db(clean_users_df, table_name, upload_engine)

# info
print("...info")
clean_users_df.info()


# *** cards data
print("*** processing cards_data")

# extract card data
print("...extracting")
url = extractor.PDF_URL
cards_df = extractor.retrieve_pdf_data(url)

# clean card data
print("...cleaning")
clean_card_df = cleaning_util.clean_card_data(cards_df)

# upload card data
print("...uploading")
table_name = "dim_card_details"
upload_engine = connection.init_upload_db_engine()
connection.upload_to_db(clean_card_df, table_name, upload_engine)

# info
print("...info")
clean_card_df.info()


# *** stores data
print("*** processing stores_data")

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

# info
print("...info")
clean_stores_df.info()


# *** products data
print("*** processing cards_data")

# extract product data
print("...extracting")
s3_address = extractor.S3_ADDRESS
products_df = extractor.extract_from_s3(s3_address)

# clean products data
print("...cleaning")
clean_products_df = cleaning_util.clean_products_data(products_df)

# upload products data
print("...uploading")
table_name = "dim_products"
upload_engine = connection.init_upload_db_engine()
connection.upload_to_db(clean_products_df, table_name, upload_engine)

# info
print("...info")
clean_products_df.info()


# *** orders data
print("*** processing orders_data")

# extract orders data
print("...extracting")
extract_engine = connection.init_db_engine()
table_name = "orders_table"
orders_df = extractor.read_rds_table(table_name, extract_engine)

# clean orders data
print("...cleaning")
clean_orders_df = cleaning_util.clean_orders_data(orders_df)

# upload orders data
print("...uploading")
table_name = "orders_table"
upload_engine = connection.init_upload_db_engine()
connection.upload_to_db(clean_orders_df, table_name, upload_engine)

# info
print("...info")
clean_orders_df.info()


# *** date date events
print("*** processing orders_data")

# extract date events
print("...extracting")
file_address = config("DATE-EVENTS-DATA-LINK")
date_events_df = extractor.extract_date_events_data(file_address)

# clean date events
print("...cleaning")
clean_date_events_df = cleaning_util.clean_date_events(date_events_df)

# upload date events
print("...uploading")
table_name = "dim_date_times"
upload_engine = connection.init_upload_db_engine()
connection.upload_to_db(clean_date_events_df, table_name, upload_engine)

# info
print("...info")
clean_date_events_df.info()


# end
print("end")
