import requests
import pandas as pd
import os
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from dotenv import load_dotenv

load_dotenv()

class MongoDatabase:

    def __init__(self, db_name, collection_name):
        self.uri = os.getenv("MONGO_URI")

        if not self.uri:
            raise ValueError("Environment variable not found in .env file: MONGO_URI")
        
        self.client = None
        self.db_name = db_name
        self.collection_name = collection_name
    
    def connectMongo(self):

        self.client = MongoClient(self.uri, server_api=ServerApi('1'))

        # Send a ping to confirm a successful connection
        try:
            self.client.admin.command('ping')
            print("Information: You've connected to Mongo Atlas successfully!")
        except Exception as e:
            print(e)

    def get_db(self):

        if not self.client:
            raise ValueError("The connection with Mongo Atlas was not established. Please connect to Mongo Atlas before creating a database.")

        return self.client[self.db_name]
    
    def get_collection(self, db):

        return db[self.collection_name]
    
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
            
            if new_name in self.get_index_names(self.collection):

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
