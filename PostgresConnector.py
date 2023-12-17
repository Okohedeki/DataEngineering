import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_batch
import polars as pl
import logging 

from dotenv import load_dotenv
import os
import traceback
from RequestLogger import DatabaseLoggingHandler

load_dotenv()  # This will load the environment variables from the .env file



class PostgresConnector:
    def __init__(self):
        self.db_config = self.set_db_config()
        self.unique_columns = None
        self.connection = None

    def set_db_config(self):
            return {
                'dbname': os.getenv('DB_NAME', 'default_db_name'),
                'user': os.getenv('DB_USER', 'default_user'),
                'password': os.getenv('DB_PASSWORD', 'default_password'),
                'host': os.getenv('DB_HOST', 'default_host')
            }

        

    def set_unique_columns(self, unique_columns=None):
            """
            Set or reset the unique columns for the inserter.

            :param unique_columns: List of column names to be set as unique, or None to reset.
            """
            self.unique_columns = unique_columns if unique_columns is not None else []

    def connect(self):
        try:
            self.connection = psycopg2.connect(**self.db_config)
            print("Database connection established.")
        except Exception as e:
            print("Error connecting to the database:", e)
            traceback.print_exc()


    def insert_batch_data(self, table, data, page_size=100):
        if not self.connection:
            print("Not connected to the database.")
            return
            

        # database_name = self.connection.get_dsn_parameters()['dbname']
        # print(database_name)

        # cursor = self.connection.cursor()
        # cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
        # tables = [table[0] for table in cursor.fetchall()]
        # print(tables)
        # cursor.close()

        # time.sleep(100)

        with self.connection:
            with self.connection.cursor() as cursor:
                try:
                    # Assuming 'data' is a list of tuples or namedtuples
                    columns = data[0]._fields  # Extracting column names from namedtuples
                    placeholders = sql.SQL(', ').join(sql.Placeholder() * len(columns))  # Correct placeholders

                    if self.unique_columns:
                        query_string = "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) DO NOTHING"
                    else:
                        query_string = "INSERT INTO {} ({}) VALUES ({})"
                        self.unique_columns = []



                    query = sql.SQL(query_string).format(
                        sql.Identifier(table),
                        sql.SQL(', ').join(map(sql.Identifier, columns)),
                        placeholders,
                        sql.SQL(', ').join(map(sql.Identifier, self.unique_columns))
                    )
                    execute_batch(cursor, query, data, page_size=page_size)
                    print("Batch data inserted successfully.")
                except Exception as e:
                    print("Error inserting batch data:", e, self.unique_columns)
                    traceback.print_exc()



    def read_table_into_polars_dataframe(self, table):
        """ Reads data from a specified table into a Polars DataFrame. """
        if not self.connection:
            print("Not connected to the database.")
            return None

        with self.connection.cursor() as cursor:
            try:
                query = f"SELECT * FROM {table};"
                cursor.execute(query)
                columns = [desc[0] for desc in cursor.description]
                data = cursor.fetchall()
                return pl.DataFrame({col: [row[i] for row in data] for i, col in enumerate(columns)})
            except Exception as e:
                print("Error reading data into Polars DataFrame:", e)
                traceback.print_exc()

                return None

    def read_table_column_into_polars_dataframe_where(self, table, column, value):
        """ Reads data from a specified table and column into a Polars DataFrame based on a where statement. """
        if not self.connection:
            print("Not connected to the database.")
            return None

        query = sql.SQL("SELECT * FROM {} WHERE {} = %s;").format(
            sql.Identifier(table),
            sql.Identifier(column)
        )
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, (value,))
                columns = [desc[0] for desc in cursor.description]
                data = cursor.fetchall()
                return pl.DataFrame({col: [row[i] for row in data] for i, col in enumerate(columns)})
        except Exception as e:
            print("Error reading data into Polars DataFrame:", e)
            traceback.print_exc()

            return None

    def close_connection(self):
        if self.connection:
            self.connection.close()
            print("Database connection closed.")

# Usage Example
if __name__ == "__main__":
    unique_columns = ['code']  # Columns that should be unique

    connector = PostgresConnector(unique_columns)
    connector.connect()

    # # Example data for batch insertion
    # data = [('code1', 'http://example.com/1', 'Description 1'),
    #         ('code2', 'http://example.com/2', 'Description 2'),
    #         ('code3', 'http://example.com/3', 'Description 3')]

    # inserter.insert_batch_data('your_table', data, page_size=100)

    pl_dataframe = connector.read_table_into_polars_dataframe('icd10_codes')
    if pl_dataframe is not None:
        print(pl_dataframe.head(10))

    connector.close_connection()