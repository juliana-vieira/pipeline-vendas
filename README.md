# Pipeline utilizando MongoDB em nuvem

Este repositório implementa um pipeline em Python que extrai dados de APIs e armazena em um banco de dados MongoDB Atlas, o banco de dados Mongo na nuvem. O pipeline realiza transformações nos dados (como renomear colunas, formatar datas), converte os dados para um DataFrame e os salva em formato CSV. O pipeline utiliza técnicas de Programação Orientada a Objetos (POO) para abstrair os processos e possibilita a extração de dados de qualquer API, bastando configurar um dicionário `config` com as informações necessárias.

Projeto realizado na formação de "Primeiros passos em Engenharia de Dados" na Alura, com algumas modificações e melhorias.

## Funcionalidades

-   **Extração de dados de API**: O pipeline é configurado para extrair dados de qualquer API que retorne um JSON.
-   **Carregamento de dados no MongoDB**: Os dados extraídos são armazenados em um banco de dados na nuvem utilizando o MongoDB Atlas.
-   **Transformação de Dados**:
    -   Renomeação de índices.
    -   Formatação de datas para o tipo `datetime` e no formato desejado.
	-   Conversão para DataFrame: Converte os dados extraídos para um DataFrame utilizando a biblioteca `pandas`.
	-   Exportação para CSV: Salva os dados em arquivos CSV após as transformações.

## Estrutura do Código

O pipeline é composto pelas classes `MongoDatabase` e `Data`, que abstraem os processos de conexão com o banco de dados, extração de dados da API, manipulação de dados e exportação para CSV.

### Classes:

1. **Classe `MongoDatabase`:**

- `MongoDatabase(username, cluster_name)`: Método construtor. É necessário instanciar a classe `MongoDatabase` para criar o Mongo Client. Informe o usuário do Mongo e o nome do cluster a partir do dicionário de configuração `config`. Armazene a senha e a URL do cluster nas variáveis de ambiente `DB_PASSWORD` e `CLUSTER_URL` no arquivo de ambiente `.env`.
 - `connectMongo()`: Necessário para se conectar no Mongo Atlas.
 - `get_db(db_name)`: Cria um banco de dados no MongoDB Atlas. Se o banco de dados já existir, o método se conecta nesse banco de dados.
 - `get_collection(collection_name)`: Cria uma coleção no banco de dados. Se a coleção já existir, o método se conecta nessa coleção. Armazene a coleção em uma variável para utilizá-la nos métodos de transformação da classe `Data`, como `collection = get_collection(collection_name)`.
 - `close_connection()`: Após realizar todas as operações no banco de dados, utilize esse método para se desconectar.

2. **Classe `Data`:**

- `Data()`: Método construtor. É necessário instanciar a classe Data para utilizar seus métodos, como `dados = Data()`.
 - `extract_api_data(endpoint)`: Extrai os dados de qualquer API que retorne um JSON. É necessário informar o `endpoint` da API no dicionário de configuração `config`.
 - `insert_data_db(collection)`: Insere os dados extraídos na coleção do banco de dados. É necessário informar a coleção que deseja inserir os dados.
 - `get_index_names(collection)`: Retorna as chaves dos documentos do banco de dados da coleção.
 - `get_collection_data(collection)`: Retorna todos os documentos do banco de dados da coleção.
 - `rename_index(collection, index_name, new_name)`: Renomeia o índice indicado da coleção.
 - `select_items(collection, query_type, field, item)`: Seleciona os dados da coleção de acordo com o tipo de operação (string ou regex) e os filtros.
 - `to_dataframe(dados)`: Transforma os dados em um dataframe pandas.
 - `format_date(dataframe, column_date, date_format)`: Formata colunas de datas para o formato desejado e converte o tipo de variável para `datetime64[ns]`.
 - `save_csv(dataframe, path)`: Salva o dataframe pandas em um arquivo csv no path desejado.

## Instalação

-   Clone este repositório:
    
    ```bash 
    git clone https://github.com/juliana-vieira/pipeline-vendas.git
    
-   Instale as dependências:
    ```bash
    pip install -r requirements.txt
    
   **Requisitos**:
    
  *   `pandas`
  *   `requests`
  *  `pymongo`
  *  `python-dotenv`

## Como Usar

-   **Configuração do Dicionário `config`**: O dicionário contém as informações necessárias para conectar à API e ao MongoDB Atlas:
    
    ```python
    config = {
        "username": "seu_usuario",
        "cluster_name": "seu_cluster",
        "url_api": "https://exemplo.com/api/dados",
        "db_name": "nome_do_banco",
        "collection_name": "nome_da_colecao"
    }
    
-   **Executando o Pipeline**: O código principal realiza as seguintes etapas:
    
    -   Conexão ao MongoDB Atlas.
    -   Extração de dados da API.
    -   Inserção dos dados no MongoDB.
    -   Renomeia colunas e transforma os dados de acordo com as necessidades.
    -   Converte os dados para DataFrame e os salva em um arquivo CSV.
      
    
    ```python
    
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
    
    # Criando o banco de dados e a coleção. Se já existir, o método irá apenas se conectar ao banco e à coleção
    db = mongo_client.get_db(config["db_name"])
    collection = mongo_client.get_collection(db, config["collection_name"])
    
    # Extraindo os dados da API e inserindo no banco
    dados = Data()
    dados.extract_api_data(config["url_api"])
    dados.insert_data_db(collection)
    
    # Extraindo informações sobre os índices da base de dados
    nomes_index = dados.get_index_names(collection)
    print(f"Nomes dos índices: {nomes_index}\n")
    
    # Transformando os dados - renomeando os índices
    dados.rename_index(collection, "lat", "Latitude")
    dados.rename_index(collection, "lon", "Longitude")

    # Verificando a transformação
    nomes_index = dados.get_index_names(collection)
    print(f"Nomes dos índices: {nomes_index}\n")
    
    # Filtrando os dados desejados
    categoria_livros = dados.select_items(collection, "string", "Categoria do Produto", "livros")
    dados_filtrados = dados.select_items(collection, "regex", "Data da Compra", "/202[1-9]")

    # Fechando a conexão com o banco
    mongo_client.close_connection()
    
    # Convertendo os filtros para dataframes pandas
    df_livros = dados.to_dataframe(categoria_livros)
    df_2021_em_diante = dados.to_dataframe(dados_filtrados)
    
    # Formatando as datas para datetime e armazenando num dicionário de dataframes
    df_formatados = {"df_livros" : dados.format_date(df_livros, "Data da Compra", "%Y-%m-%d"),
                     "df_2021_em_diante": dados.format_date(df_2021_em_diante, "Data da Compra", "%Y-%m-%d")}
    
    # Salvando os dataframes em csv
    for key, item in df_formatados.items():
         dados.save_csv(item, f"data/{key}.csv")

Esse foi apenas um exemplo com uma base de dados de vendas de produtos. O código acima pode ser modificado para se encaixar em diferentes base de dados de APIs que retornem um arquivo JSON, basta modificar o dicionário de configuração e utilizar os métodos disponíveis da classe Data para realizar as operações que precisar!

🎉 **Obrigada por conferir este projeto!** 😊
