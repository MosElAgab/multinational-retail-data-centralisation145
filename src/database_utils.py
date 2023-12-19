import yaml
from sqlalchemy import create_engine, URL
from pandas import DataFrame


class DatabaseConnector():
    # init
    def __init__(self):
        self.creds_url = "./db_creds.yaml"
        self.upload_creds_url = "./local_db_creds.yaml"

    # load database credentials method
    def __read_db_creds(self):
        with open(self.creds_url, "r") as f:
            creds = yaml.safe_load(f)
        return creds

    # initiate db connection engine method
    def init_db_engine(self):
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

    def __read_upload_db_creds(self):
        with open(self.upload_creds_url, "r") as f:
            creds = yaml.safe_load(f)
        return creds

    def init_upload_db_engine(self):
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
        df.to_sql(name=table_name, con=engine, if_exists="replace")
