import mysql.connector
import os
from dotenv import load_dotenv

load_dotenv()

class MysqlDatabase:

    def __init__(self) -> None:
         self.host = os.getenv("MYSQL_HOST")
         self.username = os.getenv("MYSQL_USERNAME")
         self.password = os.getenv("MYSQL_PASSWORD")

         if not self.host or not self.username or not self.password:
             raise ValueError("Environment variables not found in .env file.")

         self.cursor = None
         self.db_name = None

    def connect_mysql(self) -> object:

         cnx  = mysql.connector.connect(
             host = self.host,
             user = self.username,
             password = self.password
         )
        
         return cnx

    def create_cursor(self, cnx) -> object:

         self.cursor = cnx.cursor()
         return self.cursor

    def create_database(self, db_name: str) -> None:

        self.db_name = db_name

        try:
            self.cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name};")
            print("Success")

        except Exception as e:
            print(e)

    def show_databases(self) -> None:

        self.cursor.execute("SHOW DATABASES;")
        for db in self.cursor:
            print(db)

    def create_tables(self, tb_name: str, columns: dict) -> None:

        query = """
                CREATE TABLE IF NOT EXISTS %s.%s (
                    PRIMARY KEY (_id),
                """ % (self.db_name, tb_name)
        
        sql_columns = []

        for column_name, column_type in columns.items():
            sql_columns.append(f"{column_name} {column_type}")
             
        query += ", ".join(sql_columns) + ");"

        try:
            self.cursor.execute(query)
            print("MySQL Table created successfully!")

        except Exception as e:
            print(e)
    
    def insert_data_mysql(self, cnx: object, tb_name: str, lista_dados: list) -> None:
        qtd_columns = ', '.join(['%s'] * len(lista_dados[0]))

        sql = f"INSERT IGNORE INTO {self.db_name}.{tb_name} VALUES ({qtd_columns});"

        try:
            self.cursor.executemany(sql, lista_dados)
            cnx.commit()
            print(f"Data inserted successfully into the database: {self.db_name}")
        except Exception as e:
            print(e)
            cnx.rollback()