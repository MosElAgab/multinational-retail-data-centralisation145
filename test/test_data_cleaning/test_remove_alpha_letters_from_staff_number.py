import numpy as np
from src.data_cleaning import DataCleaning

cleaning_util = DataCleaning()

def test_remove_alpha_letters_from_staff_number_skips_nans():
    sample = np.nan
    expected = np.nan
    result = cleaning_util.remove_alpha_letters_from_staff_number(sample)
    assert result is expected


def test_remove_alpha_letters_from_staff_number_skips_valid_valus():
    sample = "320"
    expected = "320"
    result = cleaning_util.remove_alpha_letters_from_staff_number(sample)
    assert result == expected
    sample = 46
    expected = "46"
    result = cleaning_util.remove_alpha_letters_from_staff_number(sample)
    assert result == expected


def test_remove_alpha_letters_from_staff_number():
    sample = "J78"
    expected = "78"
    result = cleaning_util.remove_alpha_letters_from_staff_number(sample)
    assert result == expected
    sample = "80R"
    expected = "80"
    result = cleaning_util.remove_alpha_letters_from_staff_number(sample)
    assert result == expected
