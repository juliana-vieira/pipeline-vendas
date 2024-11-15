import requests
import pandas as pd
import os
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from dotenv import load_dotenv

load_dotenv()

class MongoDatabase:

    def __init__(self, username, cluster_name):
        self.db_password = os.getenv("DB_PASSWORD")
        self.cluster_url =  os.getenv("CLUSTER_URL")

        if not self.db_password or not self.cluster_url:
            raise ValueError("Environment variables not found in .env file.")
        
        self.uri = f"mongodb+srv://{username}:{self.db_password}@{self.cluster_url}/?retryWrites=true&w=majority&appName={cluster_name}"
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

class Data:

    def __init__(self):
        self.data = None

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

    def insert_data_db(self, collection):
            
        if self.data is None:
            raise ValueError("No data was extracted.")
        
        elif collection.count_documents({}) > 0:
            print("Information: This collection is already populated. If you need to insert more data, please create another collection.")
            return
        
        docs = collection.insert_many(self.data)
        print("The data was inserted successfully!")

        return docs
    
    @staticmethod
    def get_index_names(collection):

        try:
            agg = [
                {"$project": {"keys": {"$objectToArray": "$$ROOT"}}},
                {"$unwind": "$keys"},
                {"$group": {"_id": None, "distinctKeys": {"$addToSet": "$keys.k"}}},
            ]

            result = list(collection.aggregate(agg))

            return result[0]["distinctKeys"]
        
        except Exception as e:
            print(e)
    
    @staticmethod
    def get_collection_data(collection):

        doc_list = []

        try:
            for doc in collection.find():
                doc_list.append(doc)

            return doc_list
        
        except Exception as e:
            print(e)

    def rename_index(self, collection, index_name, new_name):
        try:
            
            if new_name in self.get_index_names(collection):

                print(f'The column name "{new_name}" is already present in the database.')
                return
            else:

                collection.update_many({}, {"$rename" : {index_name : new_name}})
            print(f'Column renamed successfully from "{index_name}" to "{new_name}"')

        except Exception as e:
            print(e)

    @staticmethod
    def select_items(collection, query_type, field, item):

        result = []

        try:
            if query_type == "regex":
                query = {field : {"$regex" : item}}

            elif query_type == "string":
                query = {field : item}

            for doc in collection.find(query):
                result.append(doc)

            return result
        
        except Exception as e:
            print(e)
    
    @staticmethod
    def to_dataframe(df):

        df = pd.DataFrame(df)

        return df
    
    @staticmethod
    def format_date(df, column_date, date_format):

        try:
            df[column_date] = pd.to_datetime(df[column_date], format = "%d/%m/%Y")
            df[column_date] = df[column_date].dt.strftime(date_format)
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
