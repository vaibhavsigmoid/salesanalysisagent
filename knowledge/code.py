from typing import Optional

import mysql.connector
import pandas as pd
from mysql.connector import Error


class CSVReader:
    def __init__(self, file_path: str):
        self.file_path = file_path

    def read_csv(self) -> pd.DataFrame:
        try:
            df = pd.read_csv(self.file_path)
            print(f"CSV loaded with shape: {df.shape}")
            return df
        except Exception as e:
            raise RuntimeError(f"Failed to read CSV file: {e}")


class MySQLConnector:
    def __init__(self, host: str, user: str, password: str, database: str):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.connection: Optional[mysql.connector.MySQLConnection] = None

    def connect(self):
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database,
            )
            if self.connection.is_connected():
                print("Connected to MySQL")
        except Error as e:
            raise ConnectionError(f"Error connecting to MySQL: {e}")

    def close(self):
        if self.connection and self.connection.is_connected():
            self.connection.close()
            print("MySQL connection closed.")


class MySQLTableWriter:
    def __init__(self, connector: MySQLConnector):
        self.connector = connector

    def create_table_if_not_exists(self, table_name: str, df: pd.DataFrame):
        cursor = self.connector.connection.cursor()

        columns = []
        for col in df.columns:
            dtype = "TEXT"
            if pd.api.types.is_integer_dtype(df[col]):
                dtype = "INT"
            elif pd.api.types.is_float_dtype(df[col]):
                dtype = "FLOAT"
            columns.append(f"`{col}` {dtype}")

        columns_sql = ", ".join(columns)
        create_stmt = f"CREATE TABLE IF NOT EXISTS `{table_name}` ({columns_sql});"
        cursor.execute(create_stmt)
        cursor.close()
        print(f"Ensured table `{table_name}` exists.")

    def insert_data(self, table_name: str, df: pd.DataFrame, batch_size: int = 1000):
        cursor = self.connector.connection.cursor()
        placeholders = ", ".join(["%s"] * len(df.columns))
        columns = ", ".join([f"`{col}`" for col in df.columns])
        insert_stmt = f"INSERT INTO `{table_name}` ({columns}) VALUES ({placeholders})"

        data = df.values.tolist()

        for i in range(0, len(data), batch_size):
            batch = data[i : i + batch_size]
            cursor.executemany(insert_stmt, batch)
            self.connector.connection.commit()
            print(f"Inserted batch {i} - {i+len(batch)}")

        cursor.close()
        print("Data inserted successfully.")


def main():
    # Config
    file_path = "data.csv"  # Update this to your CSV file
    host = "127.0.0.1"
    user = "root"
    password = "password"
    database = "pos_data"
    table_name = "sales_data"

    # Process
    csv_reader = CSVReader(file_path)
    df = csv_reader.read_csv()

    connector = MySQLConnector(host, user, password, database)
    connector.connect()

    writer = MySQLTableWriter(connector)
    writer.create_table_if_not_exists(table_name, df)
    writer.insert_data(table_name, df)

    connector.close()


if __name__ == "__main__":
    main()
