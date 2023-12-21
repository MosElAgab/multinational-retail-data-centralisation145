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
        # clean first name
        users_df["first_name"] = users_df["first_name"].apply(
             self.replace_invalid_name_with_nan
             )
        # clean last name
        users_df["last_name"] = users_df["last_name"].apply(
             self.replace_invalid_name_with_nan
             )
        # drop rows where first/last_name is nan
        users_df = self.drop_rows_where_name_is_nan(users_df)
        # clean date_of_birth
        users_df["date_of_birth"] = users_df["date_of_birth"].apply(
             self.fix_date_format
             )
        # clean email address
        cleaned_emails = users_df['email_address'].str.replace("@@", '@')
        users_df["email_address"] = cleaned_emails.apply(
             self.replace_invalid_email_with_nan
             )
        # clean country_code
        users_df["country_code"] = users_df["country"].apply(
             self.assign_valid_country_code
             )
        # clean phone_number
        users_df["phone_number"] = users_df["phone_number"].apply(
             self.replace_invalid_phone_numbers_with_nan
             )
        users_df["phone_number"] = users_df["phone_number"].str.replace(
            'x',
            ''
            )
        # clean join_date
        users_df["join_date"] = users_df["join_date"].apply(
             self.fix_date_format
             )
        # Drop rows with less than 5 non-null values
        users_df.dropna(thresh=5, inplace=True)
        # update index
        users_df['index'] = self.generate_index_list(users_df)
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
        cards_df.loc[:, "date_payment_confirmed"] = fixed_dates
        cards_df["index"] = self.generate_index_list(cards_df)
        cards_df.set_index("index", inplace=True)
        return cards_df

    def clean_store_data(self, stores_df):
        stores_df = self.replace_null_with_nan(stores_df)
        clean_store_type = stores_df["store_type"].apply(
            self.replace_invalid_store_type_with_nan
        )
        stores_df.loc[:, "store_type"] = clean_store_type
        stores_df = self.drop_rows_where_store_type_is_nan(stores_df)
        stores_df = stores_df.drop(columns="lat")
        clean_staff_numbers = stores_df["staff_numbers"].apply(
            self.remove_alpha_letters_from_staff_number
        )
        stores_df.loc[:, "staff_numbers"] = clean_staff_numbers
        fixed_opening_dates = stores_df["opening_date"].apply(
            self.fix_date_format
        )
        stores_df.loc[:, "opening_date"] = fixed_opening_dates
        fixed_country_code = stores_df["country_code"].str.replace("GB", "UK")
        stores_df.loc[:, "country_code"] = fixed_country_code
        fixed_continent = stores_df["continent"].str.replace("eeEurope", "Europe")
        stores_df.loc[:, "continent"] = fixed_continent
        fixed_continent = stores_df["continent"].str.replace("eeAmerica", "America")
        stores_df.loc[:, "continent"] = fixed_continent
        stores_df["index"] = self.generate_index_list(stores_df)
        stores_df.set_index("index", inplace=True)
        return stores_df

    def replace_null_with_nan(self, df):
        return df.replace("NULL", np.nan)

    def replace_invalid_name_with_nan(self, value):
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

    def replace_invalid_phone_numbers_with_nan(self, phone_number):
        try:
            if any(char.isalpha() and char.isupper() for char in phone_number):
                return np.nan
            else:
                return phone_number
        except TypeError:
            return phone_number

    def drop_rows_where_name_is_nan(self, df):
        mask_1 = df["first_name"].isna()
        mask_2 = df["last_name"].isna()
        mask = (mask_1 & mask_2)
        df = df[~mask]
        return df


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

    def generate_index_list(self, df):
        index = [i for i in range(len(df))]
        return index

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
    
    def drop_rows_where_store_type_is_nan(self, stores_df):
        mask = stores_df["store_type"].isna()
        return stores_df[~mask]
    
    def replace_invalid_store_type_with_nan(self, store_type):
        if store_type is np.nan:
            return store_type
        store_type_string = str(store_type)
        contain_digit = any([char.isdigit() for char in store_type_string])
        if contain_digit:
            return np.nan
        return store_type_string

    def remove_alpha_letters_from_staff_number(self, staff_number):
        if staff_number is np.nan:
            return np.nan
        staff_number_string = str(staff_number)
        contain_letters = any([char for char in staff_number_string])
        if contain_letters:
            fixed_staff_numbers = staff_number_string
            for char in staff_number_string:
                if char.isalpha():
                    fixed_staff_numbers = fixed_staff_numbers.replace(char, "")
            return int(fixed_staff_numbers)
        return staff_number
