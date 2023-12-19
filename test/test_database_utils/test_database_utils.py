from src.database_utils import DatabaseConnector

connection = DatabaseConnector()


# def test_class_loads_credentials():
#     result = connection.__read_db_creds()
#     assert isinstance(result, dict)


# TODO find a way to test connection engine
def test_class_initiates_engine():
    test_engine = connection.init_db_engine()
    assert test_engine.url.port == 5432
