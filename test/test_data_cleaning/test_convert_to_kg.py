import numpy as np
from src.data_cleaning import DataCleaning

cleaning_util = DataCleaning()


def test_it_skipps_valid_value():
    sample = "1.6kg"
    result = cleaning_util.convert_to_kg(sample)
    expected = "1.6kg"
    assert result == expected


def test_it_skips_invalid_values():
    sample = np.nan
    result = cleaning_util.convert_to_kg(sample)
    expected = np.nan
    assert result is expected
    sample = "ERGF43DS7C"
    result = cleaning_util.convert_to_kg(sample)
    expected = "ERGF43DS7C"
    assert result == expected


def test_it_converts_g_to_kg():
    sample = "125g"
    result = cleaning_util.convert_to_kg(sample)
    expected = "0.125kg"
    assert result == expected
    sample = "590g"
    result = cleaning_util.convert_to_kg(sample)
    expected = "0.59kg"
    assert result == expected


def test_it_converts_ml_to_kg():
    sample = "111ml"
    result = cleaning_util.convert_to_kg(sample)
    expected = "0.111kg"
    assert result == expected
    sample = "800ml"
    result = cleaning_util.convert_to_kg(sample)
    expected = "0.8kg"
    assert result == expected


def test_it_converts_oz_to_kg():
    sample = "16oz"
    result = cleaning_util.convert_to_kg(sample)
    expected = "0.454kg"
    assert result == expected
    sample = "12oz"
    result = cleaning_util.convert_to_kg(sample)
    expected = "0.34kg"
    assert result == expected


def test_it_converts_multiple_of_weights_in_g_to_kg():
    sample = "12 x 100g"
    result = cleaning_util.convert_to_kg(sample)
    expected = "1.2kg"
    assert result == expected
    sample = "6 x 412g"
    result = cleaning_util.convert_to_kg(sample)
    expected = "2.472kg"
    assert result == expected


def test_it_converts_misstyped_g_to_kg():
    sample = "77g ."
    result = cleaning_util.convert_to_kg(sample)
    expected = "0.077kg"
    assert result == expected
    sample = "412g   ."
    result = cleaning_util.convert_to_kg(sample)
    expected = "0.412kg"
    assert result == expected
