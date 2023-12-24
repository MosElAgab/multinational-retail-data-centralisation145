from pandas import DataFrame
from src.data_extraction import DataExtractor
from src.database_utils import DatabaseConnector
from decouple import config

connection = DatabaseConnector()
engine = connection.init_db_engine()


def test_list_db_tables():
    extractor = DataExtractor()
    tables_list = extractor.list_db_tables(engine)
    assert isinstance(tables_list, list)


def test_read_rds_table():
    extractor = DataExtractor()
    table = extractor.read_rds_table("legacy_store_details", engine)
    assert len(list(table.columns)) == 12


def test_retrieve_data_returns_pd_data_frame():
    extractor = DataExtractor()
    url = extractor.pdf_url
    result = extractor.retrieve_pdf_data(url)
    assert isinstance(result, DataFrame)


def test_retrieve_pdf_data_retrieve_all_records():
    extractor = DataExtractor()
    url = extractor.pdf_url
    result = extractor.retrieve_pdf_data(url)
    assert len(result) == 15309


def test_list_number_of_stores_retruns_integer():
    extractor = DataExtractor()
    url = extractor.NUMBER_OF_STORES_URL
    headers = extractor.HEADERS
    result = extractor.list_number_of_stores(url, headers)
    assert isinstance(result, int)


def test_list_number_of_stores_retruns_correct_number_stores():
    extractor = DataExtractor()
    url = extractor.NUMBER_OF_STORES_URL
    headers = extractor.HEADERS
    result = extractor.list_number_of_stores(url, headers)
    assert result == 451


# def test_retrieve_store_data_retruns_pandas_dataframe():
#     extractor = DataExtractor()
#     url = extractor.NUMBER_OF_STORES_URL
#     headers = extractor.HEADERS
#     number_of_stores = extractor.list_number_of_stores(url, headers)
#     url = extractor.STORE_DATA_URL
#     headers = extractor.HEADERS
#     result = extractor.retrieve_store_data(url, headers, number_of_stores)
#     assert isinstance(result, DataFrame)


# def test_retrieve_store_data_retruns_all_stores():
#     extractor = DataExtractor()
#     url = extractor.NUMBER_OF_STORES_URL
#     headers = extractor.HEADERS
#     number_of_stores = extractor.list_number_of_stores(url, headers)
#     url = extractor.STORE_DATA_URL
#     headers = extractor.HEADERS
#     result = extractor.retrieve_store_data(url, headers, number_of_stores)
#     assert len(result) == number_of_stores


def test_parse_s3_address_retruns_dictionary():
    extractor = DataExtractor()
    s3_address = "s3://my_bucket/data.csv"
    result = extractor.parse_s3_address(s3_address)
    assert isinstance(result, dict)


def test_parse_s3_address_returns_valid_dictionary_keys():
    extractor = DataExtractor()
    s3_address = "s3://my_bucket/data.csv"
    result = extractor.parse_s3_address(s3_address)
    assert "BUCKET_NAME" in result.keys()
    assert "KEY" in result.keys()


def test_parse_s3_address_returns_valid_bucket_name():
    extractor = DataExtractor()
    s3_address = "s3://my_bucket/data.csv"
    result = extractor.parse_s3_address(s3_address)
    assert result["BUCKET_NAME"] == "my_bucket"


def test_parse_s3_address_returns_valid_bucket_key():
    extractor = DataExtractor()
    s3_address = "s3://my_bucket/data.csv"
    result = extractor.parse_s3_address(s3_address)
    assert result["KEY"] == "data.csv"
    s3_address = "s3://my_bucket/books/data.csv"
    result = extractor.parse_s3_address(s3_address)
    assert result["KEY"] == "books/data.csv"


def test_extract_from_s3_retruns_pd_dataframe():
    extractor = DataExtractor()
    s3_address = config("S3_ADDRESS")
    result = extractor.extract_from_s3(s3_address=s3_address)
    assert isinstance(result, DataFrame)


def test_extract_from_s3_returns_all_product_data():
    extractor = DataExtractor()
    s3_address = config("S3_ADDRESS")
    print(s3_address)
    result = extractor.extract_from_s3(s3_address=s3_address)
    assert len(result) == 1853

