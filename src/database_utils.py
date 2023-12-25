from sqlalchemy import create_engine, URL
from pandas import DataFrame
from decouple import config
import yaml


class DatabaseConnector():
    """
    DatabaseConnector class for managing connections to PostgreSQL databases.

    Attributes:
        creds_url (str): File path to the YAML file containing RDS credentials.
        upload_creds_url (str): File path to the YAML file containing local database credentials.

    Methods:
        init_db_engine(): Initialize a SQLAlchemy database engine for RDS based on provided credentials.
        init_upload_db_engine(): Initialize a SQLAlchemy database engine for local database based on
        provided credentials.
        upload_to_db(df: DataFrame, table_name: str, engine): Upload a DataFrame to the
        specified database table.
    """
    def __init__(self):
        self.creds_url = config("RDS-CREDS-FILE-PATH")
        self.upload_creds_url = config("LOCAL-DB-CREDS-FILE-PATH")

    def __read_db_creds(self):
        """
        Private method to read RDS credentials from the specified file.

        Returns:
            dict: Dictionary containing RDS credentials.
        """
        try:
            with open(self.creds_url, "r") as f:
                creds = yaml.safe_load(f)
            return creds
        except FileNotFoundError:
            raise FileNotFoundError("RDS credentials yaml file not found")
        except Exception as e:
            print(f"error reading credintials: {e}")

    def __read_upload_db_creds(self):
        """
        Private method to read local database credentials from the specified file.

        Returns:
            dict: Dictionary containing local database credentials.
        """
        try:
            with open(self.upload_creds_url, "r") as f:
                creds = yaml.safe_load(f)
            return creds
        except FileNotFoundError:
            raise FileNotFoundError("RDS credentials yaml file not found")
        except Exception as e:
            print(f"error reading credintials: {e}")

    def init_db_engine(self):
        """
        Initialize a SQLAlchemy database engine for AWS RDS based on provided credentials.

        Returns:
            Engine: SQLAlchemy engine object.
        """
        DATABASE_TYPE = "postgresql"
        DBAPI = "psycopg2"
        creds = self.__read_db_creds()
        url_object = URL.create(
            f"{DATABASE_TYPE}+{DBAPI}",
            username=creds["RDS_USER"],
            password=creds["RDS_PASSWORD"],
            host=creds["RDS_HOST"],
            database=creds["RDS_DATABASE"],
            port=creds['RDS_PORT']
        )
        engine = create_engine(url_object)
        return engine

    def init_upload_db_engine(self):
        """
        Initialize a SQLAlchemy database engine for the local database based on provided credentials.

        Returns:
            Engine: SQLAlchemy engine object.
        """
        DATABASE_TYPE = "postgresql"
        DBAPI = "psycopg2"
        creds = self.__read_upload_db_creds()
        url_object = URL.create(
            f"{DATABASE_TYPE}+{DBAPI}",
            username=creds["USER"],
            password=creds["PASSWORD"],
            host=creds["HOST"],
            database=creds["DATABASE"],
            port=creds['PORT']
        )
        engine = create_engine(url_object)
        return engine

    def upload_to_db(self, df: DataFrame, table_name: str, engine) -> None:
        """
        Upload a DataFrame to the specified database table.

        Args:
            df (DataFrame): The DataFrame to be uploaded.
            table_name (str): The name of the database table.
            engine (Engine): The SQLAlchemy engine for the database.

        Returns:
            None
        """
        df.to_sql(
            name=table_name,
            con=engine,
            if_exists="replace",
            index=False
        )
