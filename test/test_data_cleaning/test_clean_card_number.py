import numpy as np
from src.data_cleaning import DataCleaning

cleaning_util = DataCleaning()


def test_it_skips_valid_values():
    sample = "4252720361802860591"
    expected = "4252720361802860591"
    result = cleaning_util.clean_card_number(sample)
    assert result == expected


def test_it_removes_question_mark():
    sample = "?4252720361802860591"
    expected = "4252720361802860591"
    result = cleaning_util.clean_card_number(sample)
    assert result == expected
    sample = "????344132437598598"
    expected = "344132437598598"
    result = cleaning_util.clean_card_number(sample)
    assert result == expected


def test_it_ignores_nan():
    sample = np.nan
    expected = np.nan
    result = cleaning_util.clean_card_number(sample)
    assert result is expected


def test_it_removes_invalid_values():
    sample = "ABCD34EF5YU"
    expected = np.nan
    result = cleaning_util.clean_card_number(sample)
    assert result is expected
