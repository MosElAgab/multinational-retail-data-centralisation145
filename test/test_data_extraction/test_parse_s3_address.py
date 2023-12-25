from src.data_extraction import DataExtractor


def test_it_retruns_dictionary():
    extractor = DataExtractor()
    s3_address = "s3://my_bucket/data.csv"
    result = extractor.parse_s3_address(s3_address)
    assert isinstance(result, dict)


def test_it_returns_valid_dictionary_keys():
    extractor = DataExtractor()
    s3_address = "s3://my_bucket/data.csv"
    result = extractor.parse_s3_address(s3_address)
    assert "BUCKET_NAME" in result.keys()
    assert "KEY" in result.keys()


def test_it_returns_valid_bucket_name():
    extractor = DataExtractor()
    s3_address = "s3://my_bucket/data.csv"
    result = extractor.parse_s3_address(s3_address)
    assert result["BUCKET_NAME"] == "my_bucket"


def test_it_returns_valid_bucket_key():
    extractor = DataExtractor()
    s3_address = "s3://my_bucket/data.csv"
    result = extractor.parse_s3_address(s3_address)
    assert result["KEY"] == "data.csv"
    s3_address = "s3://my_bucket/books/data.csv"
    result = extractor.parse_s3_address(s3_address)
    assert result["KEY"] == "books/data.csv"
