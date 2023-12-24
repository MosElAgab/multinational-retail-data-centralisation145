import pandas as pd
import numpy as np
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

# To download store data to local drive as csv file:
# run
# extractor = DataExtractor()
# url = extractor.NUMBER_OF_STORES_URL
# headers = extractor.HEADERS
# number_of_stores = extractor.list_number_of_stores(url, headers)
# url = extractor.STORE_DATA_URL
# stores_df = extractor.retrieve_store_data(url, headers, number_of_stores)
# stores_df.to_csv("stores_data.csv", index=False)


# test it replace null value with nan method
def test_replace_null_with_nan():
    data = {
        "i": [1, 2, 3],
        "first_name": ["Ahmed", "Ali", "NULL"],
        "last_name": ["Ali", "NULL", "ahmed"],
        "date_of_birth": ["NULL", "2002-10-12", "2003-02-12"]
    }
    df = pd.DataFrame(data)
    result_df = cleaning_util.replace_null_with_nan(df)
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
    result = cleaning_util.replace_invalid_name_with_nan(sample)
    expected = np.nan
    assert result is expected
    sample = "PYCLKLLC7I"
    result = cleaning_util.replace_invalid_name_with_nan(sample)
    expected = np.nan
    assert result is expected
    sample = "Alex"
    result = cleaning_util.replace_invalid_name_with_nan(sample)
    expected = "Alex"
    assert result == expected


def test_drop_rows_where_name_is_nan():
    data = {
        "i": [1, 2, 3, 4],
        "first_name": [np.nan, "Ali", "Alex", np.nan],
        "last_name": [np.nan, np.nan, "Ahmed", np.nan],
        "year": ["2002", "2003", np.nan, "2008"]
    }
    df = pd.DataFrame(data)
    result = cleaning_util.drop_rows_where_name_is_nan(df)
    assert len(result) == 2


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


def test_drop_rows_where_store_type_is_nan():
    df = pd.DataFrame(
        {
            'store_type': [12, np.nan, np.nan, 14],
            'column_2': [np.nan, 'ab', 'cd', 'ef']
        }
    )
    df = cleaning_util.drop_rows_where_store_type_is_nan(df)
    assert len(df) == 2


def test_replace_invalid_stoe_type_with_nan_skips_nans():
    sample = np.nan
    expected = np.nan
    result = cleaning_util.replace_invalid_store_type_with_nan(sample)
    assert result is expected


def test_replace_invalid_stoe_type_with_nan_skips_valid_values():
    sample = "Super Store"
    expected = "Super Store"
    result = cleaning_util.replace_invalid_store_type_with_nan(sample)
    assert result is expected
    sample = "Local"
    expected = "Local"
    result = cleaning_util.replace_invalid_store_type_with_nan(sample)
    assert result is expected
    sample = "Web Portal"
    expected = "Web Portal"
    result = cleaning_util.replace_invalid_store_type_with_nan(sample)
    assert result is expected


def test_replace_inavalid_longitude_with_nan():
    sample = "Q1TJY8H1ZH"
    expected = np.nan
    result = cleaning_util.replace_invalid_store_type_with_nan(sample)
    assert result is expected


def test_remove_alpha_letters_from_staff_number_skips_nans():
    sample = np.nan
    expected = np.nan
    result = cleaning_util.remove_alpha_letters_from_staff_number(sample)
    assert result is expected


def test_remove_alpha_letters_from_staff_number_skips_valid_valus():
    sample = 320
    expected = 320
    result = cleaning_util.remove_alpha_letters_from_staff_number(sample)
    assert result == expected
    sample = 46
    expected = 46
    result = cleaning_util.remove_alpha_letters_from_staff_number(sample)
    assert result == expected


def test_remove_alpha_letters_from_staff_number():
    sample = "J78"
    expected = 78
    result = cleaning_util.remove_alpha_letters_from_staff_number(sample)
    assert result is expected
    sample = "80R"
    expected = 80
    result = cleaning_util.remove_alpha_letters_from_staff_number(sample)
    assert result is expected


def test_convert_product_weights_returns_pd_dataframe():
    sample_data = {"weight": [i for i in range(10)]}
    sample_df = pd.DataFrame(sample_data)
    result_df = cleaning_util.convert_product_weights(sample_df)
    assert isinstance(result_df, pd.DataFrame)


def test_convert_product_weights_skips_weights_in_kg():
    sample_data = {
        "index": [i for i in range(2)],
        "weight": ["1.6kg", "0.46kg"]
    }
    sample_df = pd.DataFrame(sample_data)
    sample_df.set_index("index", inplace=True)
    result_df = cleaning_util.convert_product_weights(sample_df)
    result = result_df.loc[0, "weight"]
    expected = "1.6kg"
    assert result == expected
    result = result_df.loc[1, "weight"]
    expected = "0.46kg"
    assert result == expected


def test_convert_product_weights_converts_g_to_kg():
    sample_data = {
        "index": [i for i in range(4)],
        "weight": ["1.6kg", "125g", "9.4kg", "590g"]
    }
    sample_df = pd.DataFrame(sample_data)
    sample_df.set_index("index", inplace=True)
    result_df = cleaning_util.convert_product_weights(sample_df)
    result = result_df.loc[1, "weight"]
    expected = "0.125kg"
    assert result == expected
    result = result_df.loc[3, "weight"]
    expected = "0.59kg"
    assert result == expected


def test_convert_product_weights_converts_ml_to_kg():
    sample_data = {
        "index": [i for i in range(4)],
        "weight": ["100ml", "125g", "9.4kg", "800ml"]
    }
    sample_df = pd.DataFrame(sample_data)
    sample_df.set_index("index", inplace=True)
    result_df = cleaning_util.convert_product_weights(sample_df)
    result = result_df.loc[0, "weight"]
    expected = "0.1kg"
    assert result == expected
    result = result_df.loc[3, "weight"]
    expected = "0.8kg"
    assert result == expected


def test_convert_product_weights_converts_oz_to_kg():
    sample_data = {
        "index": [i for i in range(5)],
        "weight": ["100ml", "16oz", "125g", "12oz", "800ml"]
    }
    sample_df = pd.DataFrame(sample_data)
    sample_df.set_index("index", inplace=True)
    result_df = cleaning_util.convert_product_weights(sample_df)
    result = result_df.loc[1, "weight"]
    expected = "0.454kg"
    assert result == expected
    result = result_df.loc[3, "weight"]
    expected = "0.34kg"
    assert result == expected


def test_convert_product_weights_replaces_invalid_values_with_nan():
    sample_data = {
        "index": [i for i in range(5)],
        "weight": ["C3NCA2CL35", "16oz", "125g", "BSDTR67VD90", "800ml"]
    }
    sample_df = pd.DataFrame(sample_data)
    sample_df.set_index("index", inplace=True)
    result_df = cleaning_util.convert_product_weights(sample_df)
    result = result_df.loc[0, "weight"]
    expected = np.nan
    assert result is expected
    result = result_df.loc[0, "weight"]
    expected = np.nan
    assert result is expected




def test_convert_to_kg_skipps_valid_value():
    sample = "1.6kg"
    result = cleaning_util.convert_to_kg(sample)
    expected = "1.6kg"
    assert result == expected


def test_convert_to_kg_skips_invalid_values():
    sample = np.nan
    result = cleaning_util.convert_to_kg(sample)
    expected = np.nan
    assert result is expected
    sample = "ERGF43DS7C"
    result = cleaning_util.convert_to_kg(sample)
    expected = "ERGF43DS7C"
    assert result == expected


def test_convert_to_kg_converts_g_to_kg():
    sample = "125g"
    result = cleaning_util.convert_to_kg(sample)
    expected = "0.125kg"
    assert result == expected
    sample = "590g"
    result = cleaning_util.convert_to_kg(sample)
    expected = "0.59kg"
    assert result == expected


def test_convert_to_kg_converts_ml_to_kg():
    sample = "111ml"
    result = cleaning_util.convert_to_kg(sample)
    expected = "0.111kg"
    assert result == expected
    sample = "800ml"
    result = cleaning_util.convert_to_kg(sample)
    expected = "0.8kg"
    assert result == expected


def test_convert_to_kg_converts_oz_to_kg():
    sample = "16oz"
    result = cleaning_util.convert_to_kg(sample)
    expected = "0.454kg"
    assert result == expected
    sample = "12oz"
    result = cleaning_util.convert_to_kg(sample)
    expected = "0.34kg"
    assert result == expected


def test_convert_to_kg_converts_multiple_of_weights_in_g_to_kg():
    sample = "12 x 100g"
    result = cleaning_util.convert_to_kg(sample)
    expected = "1.2kg"
    assert result == expected
    sample = "6 x 412g"
    result = cleaning_util.convert_to_kg(sample)
    expected = "2.472kg"
    assert result == expected


def test_convert_to_kg_converts_misstyped_g_to_kg():
    sample = "77g ."
    result = cleaning_util.convert_to_kg(sample)
    expected = "0.077kg"
    assert result == expected
    sample = "412g   ."
    result = cleaning_util.convert_to_kg(sample)
    expected = "0.412kg"
    assert result == expected


# def test_clean_card_data_returns_pd_dataframe():
#     sample = pd.DataFrame()

# test_is_invalid_time_period_retruns_boolean
# test_is_invalid_time_period_retruns_false_for_nan
# test_is_invalid_time_period_retruns_false_for_valid_values
# test_is_invalid_time_period_retruns_true_for_invalid_values
    
def test_is_invalid_time_period_retruns_boolean():
    sample = "evening"
    result = cleaning_util.is_invalid_time_period(sample)
    assert isinstance(result, bool)


def test_is_invalid_time_period_retruns_false_for_nan():
    sample = np.nan
    expected = False
    result = cleaning_util.is_invalid_time_period(sample)
    assert result is expected


def test_is_invalid_time_period_retruns_false_for_valid_values():
    sample = "Evening"
    expected = False
    result = cleaning_util.is_invalid_time_period(sample)
    assert result is expected
    sample = "Late_Hours"
    expected = False
    result = cleaning_util.is_invalid_time_period(sample)
    assert result is expected


def test_is_invalid_time_period_retruns_true_for_invalid_values():
    sample = "FIEOPTNBWZ"
    expected = True
    result = cleaning_util.is_invalid_time_period(sample)
    assert result is expected
    sample = "LUVV7GL3QQ"
    expected = True
    result = cleaning_util.is_invalid_time_period(sample)
    assert result is expected
