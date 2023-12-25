import numpy as np
from src.data_cleaning import DataCleaning

cleaning_util = DataCleaning()


def test_it_retruns_boolean():
    sample = "evening"
    result = cleaning_util.is_invalid_data_point(sample)
    assert isinstance(result, bool)


def test_it_retruns_false_for_nan():
    sample = np.nan
    expected = False
    result = cleaning_util.is_invalid_data_point(sample)
    assert result is expected


def test_it_retruns_true_for_invalid_data_points():
    sample = "FIEOPTNBWZ"
    expected = True
    result = cleaning_util.is_invalid_data_point(sample)
    assert result is expected
    sample = "LUVV7GL3QQ"
    expected = True
    result = cleaning_util.is_invalid_data_point(sample)
    assert result is expected


def test_it_retruns_false_for_everything_else():
    sample = "Allen"
    expected = False
    result = cleaning_util.is_invalid_data_point(sample)
    assert result is expected
    sample = "Late_Hours"
    expected = False
    result = cleaning_util.is_invalid_data_point(sample)
    assert result is expected
