from processamento import MongoDatabase, Data

config = {
    "url_api" : "https://labdados.com/produtos",
    "db_name" : "teste",
    "collection_name" : "colecao-teste-04"
}

# Criando o Mongo Client e se conectando no cluster do Mongo Atlas
mongo_client = MongoDatabase()
mongo_client.connectMongo()

# Criando o banco e a coleção. Caso já existam, os métodos só irão se conectar no banco e na coleção
db = mongo_client.get_db(config["db_name"])
collection = mongo_client.get_collection(db, config["collection_name"])

# Criando um objeto da coleção e extraindo os dados da API
dados = Data(collection)
dados.extract_api_data(config["url_api"])

# Inserindo a coleção no banco de dados
dados.insert_data_db()

# Extraindo informações sobre os índices da base de dados
nomes_index = dados.get_index_names()
print(f"Nomes dos índices: {nomes_index}\n")

# Transformando os dados - renomeando as colunas "lat" e "lon" para melhor entendimento
dados.rename_index("lat", "Latitude")
dados.rename_index("lon", "Longitude")
nomes_index = dados.get_index_names()
print(f"Nomes dos índices: {nomes_index}\n")

# Extraindo os dados necessários para o contexto da análise
categoria_livros = dados.select_items("string", "Categoria do Produto", "livros")
vendas_2021 = dados.select_items("regex", "Data da Compra", "/202[1-9]")

# Encerrando a conexão com o banco após a conclusão de todas as operações nele
mongo_client.close_connection()

# Convertendo os dados obtidos para um dataframe pandas
df_livros = dados.to_dataframe(categoria_livros)
df_vendas_2021 = dados.to_dataframe(vendas_2021)

# Formatando a coluna "data" para datetime
df_formatados = {"df_livros" : dados.format_date(df_livros, "Data da Compra", "%Y-%m-%d"),
                  "df_2021_em_diante": dados.format_date(df_vendas_2021, "Data da Compra", "%Y-%m-%d")}

# Salvando os dados no formato csv
for key, item in df_formatados.items():
      dados.save_csv(item, f"data/{key}.csv")