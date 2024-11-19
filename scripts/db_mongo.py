import os
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from dotenv import load_dotenv

load_dotenv()

class MongoDatabase:

    def __init__(self) -> None:
        self.uri = os.getenv("MONGO_URI")

        if not self.uri:
            raise ValueError("Environment variable not found in .env file: MONGO_URI")
        
        self.client = None
        self.collection = None
    
    def connectMongo(self) -> None:

        self.client = MongoClient(self.uri, server_api=ServerApi('1'))

        # Send a ping to confirm a successful connection
        try:
            self.client.admin.command('ping')
            print("Information: You've connected to Mongo Atlas successfully!")

        except Exception as e:
            print(e)

    def get_db(self, db_name: str) -> object:

        if not self.client:
            raise ValueError("The connection with Mongo Atlas was not established. Please connect to Mongo Atlas before creating a database.")
        
        return self.client[db_name]
    
    def get_collection(self, db: object, collection_name: str) -> object:

        self.collection = db[collection_name]
        return self.collection
    
    def insert_data_db(self, data: dict) -> None:
        
        if self.collection.count_documents({}) > 0:
            print("Information: This collection is already populated. If you need to insert more data, please create another collection.")
            return
        
        self.collection.insert_many(data)
        print("The data was inserted successfully!")

    def get_index_names(self) -> list:

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
    
    def get_collection_data(self) -> list:

        doc_list = []

        try:
            for doc in self.collection.find():
                doc_list.append(doc)

            return doc_list
        
        except Exception as e:
            print(e)

    def rename_index(self, index_name: str, new_name: str) -> None:
        try:
            
            if new_name in self.get_index_names():

                print(f'The column name "{new_name}" is already present in the database.')
                return
            else:

                self.collection.update_many({}, {"$rename" : {index_name : new_name}})
            print(f'Column renamed successfully from "{index_name}" to "{new_name}"')

        except Exception as e:
            print(e)

    def select_items(self, query_type: str, field: str, item: str) -> None:

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

    def close_connection(self) -> None:

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