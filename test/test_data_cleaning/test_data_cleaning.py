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
def test_fix_date_format_reshapes_valid_dates():
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


def test_fix_date_format_retruns_nan_for_invalid_detes():
    sample = 'ABCDER'
    expected = np.nan
    result = cleaning_util.fix_date_format(sample)
    assert result is expected


def test_fix_date_format_ignores_nans():
    sample = np.nan
    expected = np.nan
    result = cleaning_util.fix_date_format(sample)
    assert result is expected


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


def test_clean_card_number_skips_valid_values():
    sample = "4252720361802860591"
    expected = "4252720361802860591"
    result = cleaning_util.clean_card_number(sample)
    assert result == expected


def test_clean_card_number_removes_question_mark():
    sample = "?4252720361802860591"
    expected = "4252720361802860591"
    result = cleaning_util.clean_card_number(sample)
    assert result == expected
    sample = "????344132437598598"
    expected = "344132437598598"
    result = cleaning_util.clean_card_number(sample)
    assert result == expected


def test_clean_card_number_ignores_nan():
    sample = np.nan
    expected = np.nan
    result = cleaning_util.clean_card_number(sample)
    assert result is expected


def test_clean_card_number_removes_invalid_values():
    sample = "ABCD34EF5YU"
    expected = np.nan
    result = cleaning_util.clean_card_number(sample)
    assert result is expected


def test_drop_rows_where_card_number_is_nan():
    df = pd.DataFrame(
        {
            'card_number': [12, np.nan, np.nan, 14],
            'column_2': [np.nan, 'ab', 'cd', 'ef']
        }
    )
    df = cleaning_util.drop_rows_where_card_number_is_nan(df)
    assert len(df) == 2
