import numpy as np
from src.data_cleaning import DataCleaning

cleaning_util = DataCleaning()


def test_it_reshapes_dates_with_incorrect_format():
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


def test_it_retruns_nan_for_invalid_detes():
    sample = 'ABCDER'
    expected = np.nan
    result = cleaning_util.fix_date_format(sample)
    assert result is expected


def test_it_ignores_nans():
    sample = np.nan
    expected = np.nan
    result = cleaning_util.fix_date_format(sample)
    assert result is expected
