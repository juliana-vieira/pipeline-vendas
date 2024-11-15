from processamento import MongoDatabase, Data

config = {
    "username" : "julianasvieira",
    "cluster_name" : "Cluster-pipeline",
    "url_api" : "https://labdados.com/produtos",
    "db_name" : "teste",
    "collection_name" : "colecao-teste-04"
}

# Conectando no MongoDB Atlas
mongo_client = MongoDatabase(config["username"], config["cluster_name"])
mongo_client.connectMongo()

# Criando o banco de dados
db = mongo_client.get_db(config["db_name"])
collection = mongo_client.get_collection(db, config["collection_name"])

# Inserindo os dados no banco
dados = Data()
dados.extract_api_data(config["url_api"])
dados.insert_data_db(collection)

# Extraindo informações sobre os índices da base de dados
nomes_index = dados.get_index_names(collection)
print(f"Nomes dos índices: {nomes_index}\n")

# Transformando os dados - renomeando as colunas
dados.rename_index(collection, "lat", "Latitude")
dados.rename_index(collection, "lon", "Longitude")
nomes_index = dados.get_index_names(collection)
print(f"Nomes dos índices: {nomes_index}\n")

# Extraindo os dados solicitados
categoria_livros = dados.select_items(collection, "string", "Categoria do Produto", "livros")
dados_filtrados = dados.select_items(collection, "regex", "Data da Compra", "/202[1-9]")
mongo_client.close_connection()

# Convertendo para dataframe
df_livros = dados.to_dataframe(categoria_livros)
df_2021_em_diante = dados.to_dataframe(dados_filtrados)

# Formatando a data para datetime
df_formatados = {"df_livros" : dados.format_date(df_livros, "Data da Compra", "%Y-%m-%d"),
                 "df_2021_em_diante": dados.format_date(df_2021_em_diante, "Data da Compra", "%Y-%m-%d")}

# Salvando os dados
for key, item in df_formatados.items():
     dados.save_csv(item, f"data/{key}.csv")