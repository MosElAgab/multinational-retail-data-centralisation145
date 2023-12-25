import pandas as pd
import numpy as np
from src.data_cleaning import DataCleaning

cleaning_util = DataCleaning()


def test_it_replaces_null_values_with_nan():
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
