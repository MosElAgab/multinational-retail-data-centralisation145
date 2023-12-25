import pandas as pd
import numpy as np
from src.data_cleaning import DataCleaning

cleaning_util = DataCleaning()


def test_it_returns_pd_dataframe():
    sample_data = {"weight": [i for i in range(10)]}
    sample_df = pd.DataFrame(sample_data)
    result_df = cleaning_util.convert_product_weights(sample_df)
    assert isinstance(result_df, pd.DataFrame)


def test_it_skips_weights_in_kg():
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


def test_it_converts_g_to_kg():
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


def test_it_converts_ml_to_kg():
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


def test_it_converts_oz_to_kg():
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


def test_it_replaces_invalid_values_with_nan():
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
