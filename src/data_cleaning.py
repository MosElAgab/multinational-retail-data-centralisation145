from dateutil.parser import parse, ParserError
from datetime import datetime as dt
from pandas import DataFrame
import numpy as np
import pandas as pd


class DataCleaning():

    def __init__(self) -> None:
        pass

    def clean_user_data(self, users_df: DataFrame) -> DataFrame:
        df = users_df.copy()
        # drop index column
        df.drop(columns="index", inplace=True)
        # replace NULL with nans
        df = self.replace_null_with_nan(df)
        # mask invalid data by catching invalid first_name
        invalid_name_mask = df["first_name"].apply(
            self.is_invalid_data_point
        )
        # replace invalid data with nan
        df.mask(invalid_name_mask, inplace=True)
        # fix date format
        column = "date_of_birth"
        df[column] = df[column].apply(
            self.fix_date_format
        )
        column = "join_date"
        df[column] = df[column].apply(
            self.fix_date_format
        )
        # clean email address
        df["email_address"] = df["email_address"].str.replace("@@", "@")
        # # clean country_code
        df["country_code"] = df["country"].apply(
            self.assign_valid_country_code
        )
        # clean phone_number
        df["phone_number"] = df["phone_number"].str.replace("x", "")
        # drop rows where all values are nans
        df.dropna(how="all", inplace=True)
        # set index
        df["index"] = self.generate_index_list(df)
        df.set_index("index", inplace=True)
        return df

    def clean_card_data(self, cards_df: DataFrame) -> DataFrame:
        df = cards_df.copy()
        # replace NULL with nans
        df = self.replace_null_with_nan(df)
        # # clean card number
        df["card_number"] = df["card_number"].apply(
            self.clean_card_number
        )
        # mask invalid data by catching invalid card_number (nan)
        invalid_card_number_mask = df["card_number"].isna()
        # replace invalid data with nan
        df.mask(invalid_card_number_mask, inplace=True)
        # fix date format
        column = "date_payment_confirmed"
        df.loc[:, column] = df[column].apply(
            self.fix_date_format
        )
        # drop rows where all values are nans
        df.dropna(how="all", inplace=True)
        # # set index
        df["index"] = self.generate_index_list(df)
        df.set_index("index", inplace=True)
        return df

    def clean_store_data(self, stores_df: DataFrame) -> DataFrame:
        df = stores_df.copy()
        # drop index column
        df.drop(columns="index", inplace=True)
        # mask invalid data by catching invalid store type 
        column = "store_type"
        invalid_store_type_mask = df[column].apply(
            self.is_invalid_data_point
        )
        # replace invalid data with nan
        df.mask(invalid_store_type_mask, inplace=True)
        # drop lat column 
        df.drop(columns="lat", inplace=True)
        # clean staff number
        column = "staff_numbers"
        df[column] = df[column].apply(
            self.remove_alpha_letters_from_staff_number
        )
        # clean date format
        column = "opening_date"
        df[column] = df[column].apply(
            self.fix_date_format
        )
        # clean continent
        column = "continent"
        df[column] = df[column].str.replace("ee", "")
        # drop rows where all values are nans
        df.dropna(how="all", inplace=True)
        return df

    def clean_products_data(self, products_df: DataFrame) -> DataFrame:
        df = products_df.copy()
        # convert product weights to kg
        df = self.convert_product_weights(df)
        # fix date format
        column = "date_added"
        df[column] = df[column].apply(
            self.fix_date_format
        )
        # mask invalid data by catching invalid weight
        invalid_weight_mask = df["weight"].isna()
        # replace invalid data with nan
        df.mask(invalid_weight_mask, inplace=True)
       # drop rows where all values are nans
        df.dropna(how="all", inplace=True)
        # reset index
        df.reset_index(inplace=True, drop=True)
        return df

    def clean_orders_data(self, orders_df: pd.DataFrame) -> pd.DataFrame:
        df = orders_df.copy()
        # drop columns
        columns = ["first_name", "last_name", "1", "level_0", "index"]
        df.drop(columns=columns, inplace=True)
        return df

    def clean_date_events(self, date_events_df: pd.DataFrame) -> pd.DataFrame:
        df = date_events_df.copy()
        # replace NULL with nan
        df = self.replace_null_with_nan(df)
        # mask invalid data by catching invalid time_period
        mask = df["time_period"].apply(
            self.is_invalid_data_point
        )
        # replace invalid data with nan
        df.mask(mask, inplace=True)
        # drop rows where all columns are nan
        df.dropna(inplace=True, how="all")
        # reset index
        df.reset_index(inplace=True, drop=True)
        return df

    def replace_null_with_nan(self, df: DataFrame) -> DataFrame:
        return df.replace("NULL", np.nan)

    def fix_date_format(self, date: str) -> dt:
        try:
            string_date = str(date)
            string_date = parse(string_date, dayfirst=True)
            string_date = dt.strftime(string_date, "%Y-%m-%d")
            return string_date
        except ParserError:
            return np.nan
        except ValueError:
            return np.nan
        except TypeError:
            return np.nan

    def assign_valid_country_code(self, country: str) -> str:
        country_code_map = {
            "United Kingdom": "GB",
            "United States": "US",
            "Germany": "DE"
        }
        try:
            return country_code_map[country]
        except KeyError:
            return np.nan

    def generate_index_list(self, df: DataFrame) -> list:
        index = [i for i in range(len(df))]
        return index

    def clean_card_number(self, card_number: str) -> str:
        card_number_string = str(card_number)
        if card_number_string.isdigit():
            return card_number
        elif "?" in card_number_string:
            return card_number_string.replace("?", "")
        else:
            return np.nan

    def remove_alpha_letters_from_staff_number(self, staff_number: str) -> str:
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
        df = products_df.copy()
        df["weight"] = df["weight"].apply(self.convert_to_kg)
        mask = df["weight"].apply(lambda value: "kg" in str(value))
        df["weight"].mask(~mask, inplace=True)
        return df

    def convert_to_kg(self, value:str) -> str:
        try:
            value_string = str(value)
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
        value_str = str(value)
        is_single_word = len(value_str.split(" ")) == 1
        contain_digit = any([char.isdigit() for char in value_str])
        is_upper_case = value_str.isupper()
        if is_single_word and contain_digit:
            return True
        elif is_single_word and is_upper_case:
            return True
        return False
