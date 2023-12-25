from dateutil.parser import parse, ParserError
from datetime import datetime as dt
from pandas import DataFrame
import numpy as np
import pandas as pd


class DataCleaning():
    """
    DataCleaning class for cleaning various dataframes.

    Attributes:
    - COUNTRY_CODE_MAP (dict): A mapping of country names to their corresponding country codes.

    Methods:
    - clean_user_data(users_df: DataFrame) -> DataFrame
    - clean_card_data(cards_df: DataFrame) -> DataFrame
    - clean_store_data(stores_df: DataFrame) -> DataFrame
    - clean_products_data(products_df: DataFrame) -> DataFrame
    - clean_orders_data(orders_df: pd.DataFrame) -> pd.DataFrame
    - clean_date_events(date_events_df: pd.DataFrame) -> pd.DataFrame
    - replace_null_with_nan(df: DataFrame) -> DataFrame
    - fix_date_format(date: str) -> dt
    - assign_valid_country_code(country: str) -> str
    - clean_card_number(card_number: str) -> str
    - remove_alpha_letters_from_staff_number(staff_number: str) -> str
    - convert_product_weights(products_df: DataFrame) -> DataFrame
    - convert_to_kg(value: str) -> str
    - is_invalid_data_point(value: str) -> bool
    """
    COUNTRY_CODE_MAP = {
        "United Kingdom": "GB",
        "United States": "US",
        "Germany": "DE"
    }

    def __init__(self) -> None:
        pass

    def clean_user_data(self, users_df: DataFrame) -> DataFrame:
        """
        Clean user data in the DataFrame.

        Parameters:
        - users_df (DataFrame): Input DataFrame containing user data.

        Returns:
        - DataFrame: Cleaned user data.

        This method performs the following steps:
        1. Drops the 'index' column from the DataFrame.
        2. Replaces NULL values with NaN.
        3. Masks invalid user data points in the 'first_name' column using the 'is_invalid_data_point' method.
        4. Replaces invalid data points with NaN.
        5. Cleans the date format in the 'date_of_birth' and 'join_date' columns using the 'fix_date_format' method.
        6. Cleans the 'email_address' column by replacing double '@@' with a single '@'.
        7. Assigns valid country codes to the 'country_code' column using the 'assign_valid_country_code' method.
        8. Cleans the 'phone_number' column by removing 'x'.
        9. Drops rows where all values are NaN.
        10. Resets the index of the DataFrame.

        Note: The original DataFrame is not modified; a cleaned copy is returned.
        """
        df = users_df.drop(columns="index")
        df = self.replace_null_with_nan(df)

        invalid_name_mask = df["first_name"].apply(self.is_invalid_data_point)
        df.mask(invalid_name_mask, inplace=True)

        column = "date_of_birth"
        df[column] = df[column].apply(self.fix_date_format)
        column = "join_date"
        df[column] = df[column].apply(self.fix_date_format)

        column = "email_address"
        df[column] = df[column].str.replace("@@", "@")

        column = "country_code"
        df[column] = df[column].apply(self.assign_valid_country_code)

        column = "phone_number"
        df[column] = df[column].str.replace("x", "")

        df.dropna(how="all", inplace=True)
        df.reset_index(inplace=True, drop=True)
        return df

    def clean_card_data(self, cards_df: DataFrame) -> DataFrame:
        """
        Clean card data in the DataFrame.

        Parameters:
        - cards_df (DataFrame): Input DataFrame containing card data.

        Returns:
        - DataFrame: Cleaned card data.

        This method performs the following steps:
        1. Replaces NULL values with NaN.
        2. Cleans the 'card_number' column using the 'clean_card_number' method.
        3. Masks invalid card numbers with NaN.
        4. Fixes the date format in the 'date_payment_confirmed' column.
        5. Drops rows where all values are NaN.
        6. Resets the index of the DataFrame.

        Note: The original DataFrame is not modified; a cleaned copy is returned.
        """
        df = self.replace_null_with_nan(cards_df)

        column = "card_number"
        df[column] = df[column].apply(self.clean_card_number)

        invalid_card_number_mask = df["card_number"].isna()
        df.mask(invalid_card_number_mask, inplace=True)

        column = "date_payment_confirmed"
        df.loc[:, column] = df[column].apply(self.fix_date_format)

        df.dropna(how="all", inplace=True)
        df.reset_index(inplace=True, drop=True)
        return df

    def clean_store_data(self, stores_df: DataFrame) -> DataFrame:
        """
        Clean store data in the DataFrame.

        Parameters:
        - stores_df (DataFrame): Input DataFrame containing store data.

        Returns:
        - DataFrame: Cleaned store data.

        This method performs the following steps:
        1. Drops the 'index' column from the DataFrame.
        2. Masks invalid store types using the 'is_invalid_data_point' method and replaces them with NaN.
        3. Drops the 'lat' column.
        4. Cleans the 'staff_numbers' column using the 'remove_alpha_letters_from_staff_number' method.
        5. Cleans the date format in the 'opening_date' column using the 'fix_date_format' method.
        6. Cleans the 'continent' column by replacing occurrences of "ee" with an empty string.
        7. Drops rows where all values are NaN.
        8. Resets the index of the DataFrame.

        Note: The original DataFrame is not modified; a cleaned copy is returned.
        """
        df = stores_df.drop(columns="index")

        column = "store_type"
        invalid_store_type_mask = df[column].apply(self.is_invalid_data_point)
        df.mask(invalid_store_type_mask, inplace=True)

        df.drop(columns="lat", inplace=True)

        column = "staff_numbers"
        df[column] = df[column].apply(self.remove_alpha_letters_from_staff_number)

        column = "opening_date"
        df[column] = df[column].apply(self.fix_date_format)

        column = "continent"
        df[column] = df[column].str.replace("ee", "")

        df.dropna(how="all", inplace=True)
        df.reset_index(inplace=True, drop=True)
        return df

    def clean_products_data(self, products_df: DataFrame) -> DataFrame:
        """
        Clean product data in the DataFrame.

        Parameters:
        - products_df (DataFrame): Input DataFrame containing product data.

        Returns:
        - DataFrame: Cleaned product data.

        This method performs the following steps:
        1. Converts product weights to kilograms using the 'convert_product_weights' method.
        2. Fixes the date format in the 'date_added' column using the 'fix_date_format' method.
        3. Masks invalid data points in the 'weight' column by checking for NaN values.
        4. Replaces invalid data points with NaN.
        5. Drops rows where all values are NaN.
        6. Resets the index of the DataFrame.

        Note: The original DataFrame is not modified; a cleaned copy is returned.
        """
        df = self.convert_product_weights(products_df)

        column = "date_added"
        df[column] = df[column].apply(self.fix_date_format)

        invalid_weight_mask = df["weight"].isna()
        df.mask(invalid_weight_mask, inplace=True)

        df.dropna(how="all", inplace=True)
        df.reset_index(inplace=True, drop=True)
        return df

    def clean_orders_data(self, orders_df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean order data in the DataFrame.

        Parameters:
        - orders_df (pd.DataFrame): Input DataFrame containing order data.

        Returns:
        - pd.DataFrame: Cleaned order data.

        This method performs the following steps:
        1. Drops specified columns ('first_name', 'last_name', '1', 'level_0', 'index').
        2. Returns the cleaned DataFrame.

        Note: The original DataFrame is modified in place.
        """
        columns = ["first_name", "last_name", "1", "level_0", "index"]
        df = orders_df.drop(columns=columns)
        return df

    def clean_date_events(self, date_events_df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean date events data in the DataFrame.

        Parameters:
        - date_events_df (pd.DataFrame): Input DataFrame containing date events data.

        Returns:
        - pd.DataFrame: Cleaned date events data.

        This method performs the following steps:
        1. Replaces NULL values with NaN.
        2. Masks invalid data points in the 'time_period' column using the 'is_invalid_data_point' method.
        3. Replaces invalid data points with NaN.
        4. Drops rows where all columns are NaN.
        5. Resets the index of the DataFrame.

        Note: The original DataFrame is not modified; a cleaned copy is returned.
        """
        df = self.replace_null_with_nan(date_events_df)

        column = "time_period"
        invalid_time_period_mask = df[column].apply(self.is_invalid_data_point)
        df.mask(invalid_time_period_mask, inplace=True)

        df.dropna(inplace=True, how="all")
        df.reset_index(inplace=True, drop=True)
        return df

    def replace_null_with_nan(self, df: DataFrame) -> DataFrame:
        """
        Replace occurrences of "NULL" with NaN in the DataFrame.

        Parameters:
        - df (DataFrame): Input DataFrame.

        Returns:
        - DataFrame: DataFrame with "NULL" replaced by NaN.

        This method replaces all occurrences of the string "NULL" in the DataFrame
        with NaN to handle missing or undefined values.

        Note: The original DataFrame is not modified; a cleaned copy is returned.
        """
        return df.replace("NULL", np.nan)

    def fix_date_format(self, date: str) -> dt:
        """
        Fix the date format in a string.

        Parameters:
        - date (str): Input date string.

        Returns:
        - dt: Date object or NaN if the input is not a valid date.

        This method attempts to parse the input date string and convert it to the
        format "%Y-%m-%d". If the parsing fails, it returns NaN.

        Note: The original date string is expected to be in a parseable format.
        """
        try:
            string_date = str(date)
            string_date = parse(string_date, dayfirst=True)
            string_date = dt.strftime(string_date, "%Y-%m-%d")
            return string_date
        except (ParserError, ValueError, TypeError):
            return np.nan

    def assign_valid_country_code(self, country: str) -> str:
        """
        Assign a valid country code based on the provided country name.

        Parameters:
        - country (str): Input country name.

        Returns:
        - str: Valid country code or NaN if the country is not found.

        This method looks up the provided country name in the predefined
        country code map (COUNTRY_CODE_MAP) and returns the corresponding
        country code. If the country is not found in the map, it returns NaN.
        """
        country_code_map = self.COUNTRY_CODE_MAP
        try:
            return country_code_map[country]
        except KeyError:
            return np.nan

    def clean_card_number(self, card_number: str) -> str:
        """
        Clean the card number string.

        Parameters:
        - card_number (str): Input card number string.

        Returns:
        - str: Cleaned card number or NaN if the input is invalid.

        This method cleans the input card number string by removing any
        non-digit characters. If the cleaned string contains only digits,
        it is returned as is. If the cleaned string contains a question
        mark ('?'), the question mark is removed. If the cleaned string
        is not a valid card number, NaN is returned.
        """
        card_number_string = str(card_number)
        if card_number_string.isdigit():
            return card_number
        elif "?" in card_number_string:
            return card_number_string.replace("?", "")
        else:
            return np.nan

    def remove_alpha_letters_from_staff_number(self, staff_number: str) -> str:
        """
        Remove alpha letters from the staff number string.

        Parameters:
        - staff_number (str): Input staff number string.

        Returns:
        - str: Staff number with alpha letters removed or NaN if the input is NaN.

        This method removes any alphabetical letters from the input staff number string.
        If the input is NaN, it returns NaN. If the staff number contains alphabetical
        letters, they are removed. If no alphabetical letters are found, the original
        staff number string is returned.
        """
        if staff_number is np.nan:
            return np.nan
        staff_number_string = str(staff_number)
        contain_letters = any([char for char in staff_number_string])
        if contain_letters:
            fixed_staff_numbers = staff_number_string
            for char in staff_number_string:
                if char.isalpha():
                    fixed_staff_numbers = fixed_staff_numbers.replace(char, "")
            return fixed_staff_numbers
        return staff_number_string

    def convert_product_weights(self, products_df: DataFrame) -> DataFrame:
        """
        Convert product weights to kilograms in the DataFrame.

        Parameters:
        - products_df (DataFrame): Input DataFrame containing product data.

        Returns:
        - DataFrame: DataFrame with product weights converted to kilograms.

        This method performs the following steps:
        1. Applies the 'convert_to_kg' method to the 'weight' column to convert weights to kilograms.
        2. Masks values in the 'weight' column that do not have 'kg' units.
        3. Returns the DataFrame with converted product weights.

        Note: The original DataFrame is modified in place.
        """
        df = products_df.copy()
        df["weight"] = df["weight"].apply(self.convert_to_kg)
        mask = df["weight"].apply(lambda value: "kg" in str(value))
        df["weight"].mask(~mask, inplace=True)
        return df

    def convert_to_kg(self, value: str) -> str:
        """
        Convert a product weight value to kilograms.

        Parameters:
        - value (str): Input value representing a product weight.

        Returns:
        - str: Converted product weight in kilograms or the original value if conversion fails.

        This method attempts to convert the input product weight value to kilograms. It supports
        various input formats, including values with units such as 'g', 'kg', 'ml', 'oz', and multiples.
        If the input value is already in kilograms ('kg'), it is returned as is. If the input value
        is in another unit or format, it is converted to kilograms.

        Note: The original value is returned if the conversion fails, and a message is printed to
        the console indicating the error.
        """
        try:
            value_string = str(value)

            # handling traling dot
            if value_string[-1] == ".":
                value_string = value_string[:(value_string.find("g") + 1)]

            if "kg" in value_string:
                return value_string
            elif " x " in value_string:
                multiple = int(value_string.split(" x ")[0])
                weight = float(value_string.split(" x ")[1].replace("g", ""))
                value_kg = (weight * multiple) / 1000
                value_kg = round(value_kg, 3)
                return str(value_kg) + "kg"
            elif "g" in value_string:
                value_kg = float(str(value_string).replace("g", "")) / 1000
                value_kg = round(value_kg, 3)
                return str(value_kg) + "kg"
            elif "ml" in value_string:
                value_kg = float(str(value_string).replace("ml", "")) / 1000
                value_kg = round(value_kg, 3)
                return str(value_kg) + "kg"
            elif "oz" in value_string:
                value_kg = float(str(value_string).replace("oz", "")) / 35.274
                value_kg = round(value_kg, 3)
                return str(value_kg) + "kg"
            else:
                return value
        except ValueError:
            print("product_data/convert_to_kg/value error", value)
            return value

    def is_invalid_data_point(self, value: str) -> bool:
        """
        Check if a data point is considered invalid.

        Parameters:
        - value (str): Input value to be checked.

        Returns:
        - bool: True if the data point is considered invalid, False otherwise.

        This method checks if a data point is considered invalid based on the following criteria:
        1. If the value is a single word and contains at least one digit.
        2. If the value is a single word and is entirely in uppercase.

        Returns True if the data point meets any of the above criteria, indicating that it is invalid.
        Otherwise, returns False.
        """
        value_str = str(value)
        is_single_word = len(value_str.split(" ")) == 1
        contain_digit = any([char.isdigit() for char in value_str])
        is_upper_case = value_str.isupper()
        if is_single_word and contain_digit:
            return True
        elif is_single_word and is_upper_case:
            return True
        return False
