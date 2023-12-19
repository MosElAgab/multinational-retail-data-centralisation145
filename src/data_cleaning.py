import pandas as pd
import numpy as np
import re
from dateutil.parser import parse, ParserError
from datetime import datetime as dt


class DataCleaning():

    def __init__(self) -> None:
        pass

    def clean_user_data(self, users_df):
        # replace nulls by nans
        users_df = self.replace_null_with_nan(users_df)
        # first name
        users_df["first_name"] = users_df["first_name"].apply(
             self.replace_invalid_values_with_nan
             )
        self.set_column_type(users_df, "first_name", "string")
        # last name
        users_df["last_name"] = users_df["last_name"].apply(
             self.replace_invalid_values_with_nan
             )
        self.set_column_type(users_df, "last_name", "string")
        # date_of_birth
        users_df["date_of_birth"] = users_df["date_of_birth"].apply(
             self.fix_date_format
             )
        self.set_column_type(users_df, "date_of_birth", "datetime64")
        # company
        users_df["company"] = users_df["company"].apply(
             self.replace_invalid_values_with_nan
             )
        self.set_column_type(users_df, "company", "string")
        # email address
        cleaned_emails = users_df['email_address'].str.replace("@@", '@')
        users_df['email_address'] = cleaned_emails
        users_df["email_address"] = users_df["email_address"].apply(
             self.replace_invalid_email_with_nan
             )
        self.set_column_type(users_df, "email_address", "string")
        # address
        users_df["address"] = users_df["address"].apply(
             self.replace_invalid_values_with_nan
             )
        self.set_column_type(users_df, "address", "string")
        # country
        users_df["country"] = users_df["country"].apply(
             self.replace_invalid_values_with_nan
             )
        self.set_column_type(users_df, "country", "string")
        # country_code
        users_df["country_code"] = users_df["country"].apply(
             self.assign_valid_country_code
             )
        self.set_column_type(users_df, "country_code", "string")
        # phone_number
        users_df["phone_number"] = users_df["phone_number"].apply(
             self.replace_invalid_phone_numbers_with_nan
             )
        users_df["phone_number"] = users_df["phone_number"].str.replace(
            'x',
            ''
            )
        self.set_column_type(users_df, "phone_number", "string")
        # join_date
        users_df["join_date"] = users_df["join_date"].apply(
             self.fix_date_format
             )
        self.set_column_type(users_df, "join_date", "datetime64")
        # user_uuid
        self.set_column_type(users_df, "user_uuid", "string")
        # Drop rows with less than 5 non-null values
        users_df.dropna(thresh=5, inplace=True)
        # update index
        users_df['index'] = self.generate_index_list(users_df)
        self.set_column_type(users_df, "index", "int64")
        users_df.set_index("index", inplace=True)
        return users_df

    def clean_card_data(self, cards_df):
        cards_df = self.replace_null_with_nan(cards_df)
        cards_df["card_number"] = cards_df["card_number"].apply(
            self.clean_card_number
        )
        cards_df = self.drop_rows_where_card_number_is_nan(cards_df)
        fixed_dates = cards_df["date_payment_confirmed"].apply(
            self.fix_date_format
        )
        cards_df["date_payment_confirmed"] = fixed_dates
        cards_df["index"] = self.generate_index_list(cards_df)
        cards_df.set_index("index", inplace=True)
        return cards_df

    def replace_null_with_nan(self, df):
        return df.replace("NULL", np.nan)

    def replace_invalid_values_with_nan(self, value):
        if value is np.nan:
            return value
        # value conditions
        one_word = len(value.split(" ")) == 1
        contain_numbers = any(char.isdigit() for char in value)
        upper_case = value.isupper()

        if one_word and contain_numbers:
            return np.nan
        elif one_word and upper_case:
            return np.nan
        else:
            return value

    def fix_date_format(self, date: str):
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

    def validate_email_address(self, email):
        pattern = r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"
        return re.match(pattern, email) is not None

    def replace_invalid_email_with_nan(self, email):
        if email is np.nan:
            return email
        elif self.validate_email_address(email):
            return email
        elif "@" in email:
            return email
        else:
            return np.nan

    def assign_valid_country_code(self, country):
        country_code_map = {
            "United Kingdom": "UK",
            "United States": "US",
            "Germany": "DE"
        }
        try:
            return country_code_map[country]
        except KeyError:
            return np.nan

    def replace_invalid_phone_numbers_with_nan(self, phone_number):
        try:
            if any(char.isalpha() and char.isupper() for char in phone_number):
                return np.nan
            else:
                return phone_number
        except TypeError:
            return phone_number

    def generate_index_list(self, df):
        index = [i for i in range(len(df))]
        return index

    def set_column_type(self, df, column_name, type):
        def to_date():
            df[column_name] = pd.to_datetime(
                df[column_name],
                format="%Y-%m-%d",
                errors='coerce'
            )
        try:
            if type == "datetime64":
                to_date()
            else:
                df[column_name] = df[column_name].astype(type, errors="raise")
        except ValueError:
            raise ValueError(f"{column_name} column type cannot be updated!")

    def clean_card_number(self, card_number):
        card_number_string = str(card_number)
        if card_number_string.isdigit():
            return card_number
        elif "?" in card_number_string:
            return card_number_string.replace("?", "")
        else:
            return np.nan

    def drop_rows_where_card_number_is_nan(self, cards_df):
        mask = cards_df['card_number'].isna()
        return cards_df[~mask]
