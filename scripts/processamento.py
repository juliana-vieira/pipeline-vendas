import requests
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from dotenv import load_dotenv
import os

load_dotenv()

class MongoDatabase:

    def __init__(self, username, cluster_name):
        self.db_password = os.getenv("DB_PASSWORD")
        self.cluster_url =  os.getenv("CLUSTER_URL")

        if not self.db_password or not self.cluster_url:
            raise ValueError("DB_PASSWORD or CLUSTER_URL not found in environment variables.")

        self.uri = f"mongodb+srv://{username}:{self.db_password}@{self.cluster_url}/?retryWrites=true&w=majority&appName={cluster_name}"
        self.client = None
    
    def connectMongo(self):

        self.client = MongoClient(self.uri, server_api=ServerApi('1'))

        # Send a ping to confirm a successful connection
        try:
            self.client.admin.command('ping')
            print("Conexão bem sucedida!")
        except Exception as e:
            print(e)

    def get_db(self, db_name):

        if self.client is None:
            self.connectMongo()

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
            response = requests.get(endpoint)
            self.data = response.json()

        except Exception as e:
            print(e)

    def insert_data(self, collection):
            
        if self.data is None:
            raise ValueError("Nenhum dado foi extraído para inserir.")
            
        docs = collection.insert_many(self.data)
        return docs
    
    @staticmethod
    def consult_data(collection):
        try:
            query = collection.find_one()
            return query
        
        except Exception as e:
            print(e)