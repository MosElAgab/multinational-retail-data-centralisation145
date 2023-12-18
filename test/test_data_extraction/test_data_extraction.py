from src.data_extraction import DataExtractor
from src.database_utils import DatabaseConnector

connection = DatabaseConnector()
engine = connection.init_db_engine()


def test_class_list_db_tables():
    data_extractor = DataExtractor()
    tables_list = data_extractor.list_db_tables(engine)
    assert isinstance(tables_list, list)


def test_class_reads_rds_table():
    data_extractor = DataExtractor()
    table = data_extractor.read_rds_table("legacy_store_details", engine)
    assert len(list(table.columns)) == 12
