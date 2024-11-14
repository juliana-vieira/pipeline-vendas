from processamento import MongoDatabase, Data

# Conectando ao Mongodb Atlas
mongo_cluster = MongoDatabase("julianasvieira", "Cluster-pipeline")
mongo_cluster.connectMongo()

# Criando o db e a collection
db = mongo_cluster.get_db("teste")
collection = mongo_cluster.get_collection(db, "colecao-teste")

# Extraindo os dados da API e inserindo no db
dados = Data()
dados.extract_api_data("https://labdados.com/produtos")
dados.insert_data(collection)
print(dados.consult_data(collection))
mongo_cluster.close_connection()