from pandas import DataFrame
from src.data_extraction import DataExtractor
from src.database_utils import DatabaseConnector

connection = DatabaseConnector()
engine = connection.init_db_engine()


def test_list_db_tables():
    data_extractor = DataExtractor()
    tables_list = data_extractor.list_db_tables(engine)
    assert isinstance(tables_list, list)


def test_read_rds_table():
    data_extractor = DataExtractor()
    table = data_extractor.read_rds_table("legacy_store_details", engine)
    assert len(list(table.columns)) == 12


def test_retrieve_data_returns_pd_data_frame():
    data_extractor = DataExtractor()
    url = data_extractor.pdf_url
    result = data_extractor.retrieve_pdf_data(url)
    assert isinstance(result, DataFrame)


def test_retrieve_pdf_data_retrieve_all_records():
    data_extractor = DataExtractor()
    url = data_extractor.pdf_url
    result = data_extractor.retrieve_pdf_data(url)
    assert len(result) == 15309

def test_list_number_of_stores_retruns_integer():
    data_extractor = DataExtractor()
    result = data_extractor.list_number_of_stores()
    assert isinstance(result, int)


def test_list_number_of_stores_retruns_correct_number_stores():
    data_extractor = DataExtractor()
    result = data_extractor.list_number_of_stores()
    assert result == 451