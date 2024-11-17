import requests
import os
import mysql.connector
import pandas as pd
import numpy as np
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from dotenv import load_dotenv

load_dotenv()

class MongoDatabase:

    def __init__(self):
        self.uri = os.getenv("MONGO_URI")

        if not self.uri:
            raise ValueError("Environment variable not found in .env file: MONGO_URI")
        
        self.client = None
    
    def connectMongo(self):

        self.client = MongoClient(self.uri, server_api=ServerApi('1'))

        # Send a ping to confirm a successful connection
        try:
            self.client.admin.command('ping')
            print("Information: You've connected to Mongo Atlas successfully!")
        except Exception as e:
            print(e)

    def get_db(self, db_name):

        if not self.client:
            raise ValueError("The connection with Mongo Atlas was not established. Please connect to Mongo Atlas before creating a database.")
        
        return self.client[db_name]
    
    @staticmethod
    def get_collection(db, collection_name):

        return db[collection_name]
    
    def close_connection(self):

        if self.client != None:
            try:
                self.client.close()
                print("MongoDB connection closed.")

            except Exception as e:
                print(f"Error closing connection: {e}")

            finally:
                self.client = None
        else:
            print("No active MongoDB connection to close.")

class MysqlDatabase:

     def __init__(self):
         self.host = os.getenv("MYSQL_HOST")
         self.username = os.getenv("MYSQL_USERNAME")
         self.password = os.getenv("MYSQL_PASSWORD")

         if not self.host or not self.username or not self.password:
             raise ValueError("Environment variables not found in .env file.")

         self.cursor = None
         self.db_name = None

     def connect_mysql(self):

         cnx  = mysql.connector.connect(
             host = self.host,
             user = self.username,
             password = self.password
         )
        
         return cnx

     def create_cursor(self, cnx):

         self.cursor = cnx.cursor()
         return self.cursor

     def create_database(self, db_name):

        self.db_name = db_name
        try:
            self.cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name};")
            print("Success")

        except Exception as e:
            print(e)


     def show_databases(self):

        self.cursor.execute("SHOW DATABASES;")
        for db in self.cursor:
            print(db)

     def create_tables(self, tb_name, columns):

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
        
class Data:

    def __init__(self, collection):
        self.data = None
        self.collection = collection

    def extract_api_data(self, endpoint):

        try:
            response = requests.get(endpoint,timeout=10)
            response.raise_for_status()

            try:
                self.data = response.json()

                if not self.data:
                    raise ValueError("A API retornou um JSON vazio.")
                
                print("Data extracted successfully from the API!")

            except ValueError as json_error:
                print(f"Erro ao processar JSON da API: {json_error}")
                self.data = None

        except requests.exceptions.RequestException as req_error:
            print(f"Erro na solicitação da API: {req_error}")
            self.data = None

    def insert_data_db(self):
            
        if self.data is None:
            raise ValueError("No data was extracted.")
        
        elif self.collection.count_documents({}) > 0:
            print("Information: This collection is already populated. If you need to insert more data, please create another collection.")
            return
        
        docs = self.collection.insert_many(self.data)
        print("The data was inserted successfully!")

        return docs
    
    def get_index_names(self):

        try:
            agg = [
                {"$project": {"keys": {"$objectToArray": "$$ROOT"}}},
                {"$unwind": "$keys"},
                {"$group": {"_id": None, "distinctKeys": {"$addToSet": "$keys.k"}}},
            ]

            result = list(self.collection.aggregate(agg))

            return result[0]["distinctKeys"]
        
        except Exception as e:
            print(e)
    
    def get_collection_data(self):

        doc_list = []

        try:
            for doc in self.collection.find():
                doc_list.append(doc)

            return doc_list
        
        except Exception as e:
            print(e)

    def rename_index(self, index_name, new_name):
        try:
            
            if new_name in self.get_index_names():

                print(f'The column name "{new_name}" is already present in the database.')
                return
            else:

                self.collection.update_many({}, {"$rename" : {index_name : new_name}})
            print(f'Column renamed successfully from "{index_name}" to "{new_name}"')

        except Exception as e:
            print(e)

    def select_items(self, query_type, field, item):

        result = []

        try:
            if query_type == "regex":
                query = {field : {"$regex" : item}}

            elif query_type == "string":
                query = {field : item}

            for doc in self.collection.find(query):
                result.append(doc)

            return result
        
        except Exception as e:
            print(e)
    
    @staticmethod
    def to_dataframe(df):

        df = pd.DataFrame(df)

        return df
    
    @staticmethod
    def dtype_to_sql(df_dict):

        sql_type = {}
        df_type = {}

        mapping = {
             np.object_: "VARCHAR(100)",
             np.float64: "FLOAT(10,2)",
             np.int64: "INT",
             np.datetime64: "DATE"
        }

        df_type = {key : df.dtypes.apply(lambda x: x.type).to_dict() for key, df in df_dict.items()}

        sql_type = {inner_key : mapping.get(inner_value) 
                    for outer_key, inner_dict in df_type.items() 
                    for inner_key, inner_value in inner_dict.items()}
        
        sql_formatted = {column_name.replace(" ", "_") : column_type for column_name, column_type in sql_type.items()}

        return sql_formatted

    @staticmethod
    def format_date(df, column_date, date_format):

        try:
            df[column_date] = pd.to_datetime(df[column_date], format = 'mixed')
            df[column_date] = df[column_date].dt.strftime(date_format)
            df[column_date] = pd.to_datetime(df[column_date], format = 'mixed')
            print("Date formatted successfully.")

            return df

        except Exception as e:
            print(e)

    @staticmethod
    def save_csv(df, path):

        try:
            df.to_csv(path)
            print("Data saved successfully as a csv file.")

        except Exception as e:
            print(e)
