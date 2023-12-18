import pandas as pd
import numpy as np
import pytest
from src.database_utils import DatabaseConnector
from src.data_extraction import DataExtractor
from src.data_cleaning import DataCleaning

# loading data
connection = DatabaseConnector()
engine = connection.init_db_engine()
extractor = DataExtractor()
cleaning_util = DataCleaning()
table_name = "legacy_users"
users_df = extractor.read_rds_table(table_name, engine)


# test it replace null value with nan method
def test_replace_null_with_nan():
    data = {
        "i": [1, 2, 3],
        "first_name": ["Ahmed", "Ali", "NULL"],
        "last_name": ["Ali", "NULL", "ahmed"],
        "date_of_birth": ["NULL", "2002-10-12", "2003-02-12"]
    }
    data_df = pd.DataFrame(data)
    result_df = cleaning_util.replace_null_with_nan(data_df)
    result = result_df.iloc[0, 3]
    expected = np.nan
    assert result is expected
    result = result_df.iloc[1, 2]
    assert result is expected
    result = result_df.iloc[2, 1]
    assert result is expected


# test replace invalid values with nan
def test_replace_invalid_name():
    sample = "WSRSCUDTR"
    result = cleaning_util.replace_invalid_values_with_nan(sample)
    expected = np.nan
    assert result is expected
    sample = "PYCLKLLC7I"
    result = cleaning_util.replace_invalid_values_with_nan(sample)
    expected = np.nan
    assert result is expected
    sample = "Alex"
    result = cleaning_util.replace_invalid_values_with_nan(sample)
    expected = "Alex"
    assert result == expected


# test fix date format
def test_fix_date_format():
    sample = '16 Oct 2020'
    expected = '2020-10-16'
    result = cleaning_util.fix_date_format(sample)
    assert result == expected
    sample = '10 October 2019'
    expected = '2019-10-10'
    result = cleaning_util.fix_date_format(sample)
    assert result == expected
    sample = '11-12-2014'
    expected = '2014-12-11'
    result = cleaning_util.fix_date_format(sample)
    assert result == expected


# test clean users data sets the correct data type for each column
def test_clean_users_data_sets_appropriate_column_data_type():
    column_data_type_map = {
        "first_name": "string",
        "last_name": "string",
        "date_of_birth": "datetime64[ns]",
        "company": "string",
        "email_address": "string",
        "address": "string",
        "country": "string",
        "country_code": "string",
        "phone_number": "string",
        "join_date": "datetime64[ns]",
        "user_uuid": "string"
    }
    cleaned_users_df = cleaning_util.clean_user_data(users_df)
    for key, value in column_data_type_map.items():
        result = cleaned_users_df[key].dtype
        expected = value
        assert result == expected


# test validate email
def test_validate_email():
    sample = "PYCLKLLC7I"
    result = cleaning_util.validate_email_address(sample)
    expected = False
    assert result is expected
    sample = "PYCLKLLCRTI"
    result = cleaning_util.validate_email_address(sample)
    expected = False
    assert result is expected
    sample = "abcd@efg@hello"
    result = cleaning_util.validate_email_address(sample)
    expected = False
    assert result is expected
    sample = "abcd@efg.hijk"
    result = cleaning_util.validate_email_address(sample)
    expected = True
    assert result is expected


# test replace invalid name with nan
def test_replace_invalid_email_with_nan():
    sample = np.nan
    expected = np.nan
    result = cleaning_util.replace_invalid_email_with_nan(sample)
    assert result is expected
    sample = "name@company.com"
    expected = "name@company.com"
    result = cleaning_util.replace_invalid_email_with_nan(sample)
    assert result == expected
    sample = "PYCLKLLC7I"
    expected = np.nan
    result = cleaning_util.replace_invalid_email_with_nan(sample)
    assert result is expected
    sample = "PYCLKLLCRTI"
    expected = np.nan
    result = cleaning_util.replace_invalid_email_with_nan(sample)
    assert result is expected


# test assign valid country code
def test_assign_valid_country_code():
    sample = "United Kingdom"
    expected = "UK"
    result = cleaning_util.assign_valid_country_code(sample)
    assert result == expected
    sample = "United States"
    expected = "US"
    result = cleaning_util.assign_valid_country_code(sample)
    assert result == expected
    sample = "Germany"
    expected = "DE"
    result = cleaning_util.assign_valid_country_code(sample)
    assert result == expected
    sample = "ACDE45ASDD"
    expected = np.nan
    result = cleaning_util.assign_valid_country_code(sample)
    assert result is expected


# test replace invalid phone number with nan
def test_replace_invalid_phone_number_with_nan():
    sample = "ACDE45ASDD"
    expected = np.nan
    result = cleaning_util.replace_invalid_phone_numbers_with_nan(sample)
    assert result is expected
    sample = "ACDE45ASDD"
    expected = np.nan
    result = cleaning_util.replace_invalid_phone_numbers_with_nan(sample)
    assert result is expected
    sample = np.nan
    expected = np.nan
    result = cleaning_util.replace_invalid_phone_numbers_with_nan(sample)
    assert result is expected
    sample = "+44(0)117 496 0576"
    expected = "+44(0)117 496 0576"
    result = cleaning_util.replace_invalid_phone_numbers_with_nan(sample)
    assert result is expected


# test generate index
def test_generate_index():
    data = {
        "index": [1, 23, 4, 6, 5, 10, 11, 12, 13, 15],
        "id": [i for i in range(10)],
        'value': ['a' for i in range(10)]
    }
    df = pd.DataFrame(data)
    result = cleaning_util.generate_index_list(df)
    # test it generates a list with same length as the df
    assert len(result) == len(df)
    # test start index
    assert result[0] == 0
    # test end index
    assert result[-1] == 9


# test set column type changes df column type
def test_set_column_type():
    data = {
        "i": [1, 2, 3],
        "first_name": ["Ahmed", "Ali", "Alex"],
        "last_name": ["Ali", "Alex", "ahmed"],
        "date_of_birth": ["2023-12-18", "2002-10-12", "12 Oct 1996"]
    }
    df = pd.DataFrame(data)
    # test with type string
    cleaning_util.set_column_type(df, "first_name", "string")
    result = df['first_name'].dtype
    expected = "string"
    assert result == expected
    # test with type datetime
    cleaning_util.set_column_type(df, "date_of_birth", "datetime64")
    result = df['date_of_birth'].dtype
    expected = "datetime64[ns]"
    assert result == expected
    # test it raises value error if column type cannot be changed
    match = "first_name column type cannot be updated!"
    with pytest.raises(ValueError, match=match):
        cleaning_util.set_column_type(df, "first_name", "int64")
        print(df)
