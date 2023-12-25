import numpy as np
from src.data_cleaning import DataCleaning

cleaning_util = DataCleaning()


def test_it_skips_nana():
    sample = np.nan
    expected = np.nan
    result = cleaning_util.assign_valid_country_code(sample)
    assert result is expected


def test_it_assings_valid_country_code():
    sample = "United Kingdom"
    expected = "GB"
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
