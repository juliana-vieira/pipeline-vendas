from api import APIHandler
from db_mongo import MongoDatabase
from db_mysql import MysqlDatabase
from processamento import DataProcessor

config = {
    "url_api" : "https://labdados.com/produtos",
    "db_name" : "teste",
    "collection_name" : "colecao"
}

# Criando o Mongo Client e se conectando no cluster do Mongo Atlas
mongo_client = MongoDatabase()
mongo_client.connectMongo()

# Criando o banco e a coleção. Caso já existam, os métodos só irão se conectar no banco e na coleção
db = mongo_client.get_db(config["db_name"])
collection = mongo_client.get_collection(db, config["collection_name"])

# Extraindo os dados da API
api_handler = APIHandler(config["url_api"])
dados_api = api_handler.extract_api_data()

# Inserindo os dados no MongoDB
mongo_client.insert_data_db(dados_api)

# Extraindo informações sobre os índices da base de dados
nomes_index = mongo_client.get_index_names()
print(f"Nomes dos índices: {nomes_index}\n")

# Transformando os dados - renomeando as colunas "lat" e "lon" para melhor entendimento
mongo_client.rename_index("lat", "Latitude")
mongo_client.rename_index("lon", "Longitude")

# Extraindo os dados necessários para o contexto da análise
categoria_livros = mongo_client.select_items("string", "Categoria do Produto", "livros")
vendas_2021 = mongo_client.select_items("regex", "Data da Compra", "/202[1-9]")

# Encerrando a conexão com o banco após a conclusão de todas as operações nele
mongo_client.close_connection()

# Convertendo os dados obtidos para um dataframe pandas
df_livros = DataProcessor.to_dataframe(categoria_livros)
df_vendas_2021 = DataProcessor.to_dataframe(vendas_2021)

#  Formatando a coluna "data" para datetime
df_formatados = {"df_livros" : DataProcessor.format_date(df_livros, "Data da Compra", "%Y-%m-%d"),
                  "df_2021_em_diante": DataProcessor.format_date(df_vendas_2021, "Data da Compra", "%Y-%m-%d")}

# Salvando os dados no formato csv
for key, item in df_formatados.items():
       DataProcessor.save_csv(item, f"data/{key}.csv")

# Conectando ao MySQL
mysql_db = MysqlDatabase()
cnx = mysql_db.connect_mysql()

# Criando um cursor para manipular o banco de dados MySQL
mysql_db.create_cursor(cnx)

# Criando o banco de dados MySQL
mysql_db.create_database("TESTE")

# Convertendo os tipos de dados para os tipos SQL
colunas_sql = DataProcessor.dtype_to_sql(df_formatados)

# Criando a tabela com os tipos convertidos
mysql_db.create_tables("tb_produtos", colunas_sql)

# Extraindo os dados dos arquivos csv e inserindo no banco de dados
dados_livros = DataProcessor.extract_csv_data("data/df_livros.csv")
dados_2021_em_diante = DataProcessor.extract_csv_data("data/df_2021_em_diante.csv")
mysql_db.insert_data_mysql(cnx, "tb_produtos", dados_livros)
mysql_db.insert_data_mysql(cnx, "tb_produtos", dados_2021_em_diante)
