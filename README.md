
# Projeto: Ingestão de Dados - SQL Server e API RESTful

## 📌 Descrição do Projeto
Este projeto em Databricks realiza a ingestão de dados de duas fontes distintas, salvando os resultados como tabelas Delta em um catálogo e schema especificados. O pipeline é projetado para ser robusto e seguro, utilizando as melhores práticas como o uso de Databricks Secrets e mecanismos de resiliência.

As fontes de dados são:

- **Banco de Dados SQL Server (AdventureWorks)**: Ingestão dinâmica de todas as tabelas.
- **API RESTful**: Ingestão paralela e paginada de múltiplos endpoints.

## ✨ Principais Funcionalidades

- **Ingestão Dinâmica do Banco**: O pipeline descobre e ingere automaticamente todas as tabelas do banco de dados de origem, sem a necessidade de listá-las manualmente.
- **Robustez na Ingestão da API**:
  - *Retries com Limites Adaptativos*: Para cada endpoint da API, o código realiza até 3 tentativas. Se uma requisição falha por timeout, o limite de registros (limit) é reduzido progressivamente (de 100 para 50, e depois para 20) para aumentar a chance de sucesso em redes instáveis.
  - *Paralelismo*: As requisições para os diferentes endpoints da API são feitas em paralelo usando ThreadPoolExecutor para otimizar o tempo de execução.
- **Sanitização de Dados**: Os nomes das colunas vindas do banco de dados são automaticamente sanitizados, substituindo espaços por underscores (ex: `Product Name` se torna `Product_Name`).
- **Segurança**: Credenciais de acesso são gerenciadas de forma segura através do Databricks Secrets.
- **Estrutura Organizada**: Os dados são salvos no Delta Lake dentro do catálogo `ted_dev` e schema `dev_guilherme_sobrinho`, com prefixos que identificam a origem (`raw_database_` ou `raw_api_`).

## ⚙️ Tecnologias Utilizadas

- Apache Spark (PySpark)
- Delta Lake
- Databricks (Notebooks, Secrets, Repos)
- Python `concurrent.futures.ThreadPoolExecutor` para paralelização.
- `requests` para consumo da API.

## 🏗️ Arquitetura da Solução

### Banco de Dados SQL Server

- A conexão é estabelecida via JDBC.
- O pipeline lê o catálogo `INFORMATION_SCHEMA.TABLES` para obter a lista de todas as tabelas do banco.
- Cada tabela é lida e suas colunas são sanitizadas antes de serem salvas.
- As tabelas são salvas com o prefixo `raw_database_`.

### API RESTful

- São ingeridos 4 endpoints específicos: `SalesOrderDetail`, `SalesOrderHeader`, `PurchaseOrderDetail` e `PurchaseOrderHeader`.
- As requisições são paginadas usando os parâmetros `offset` e `limit`.
- A autenticação é feita via Basic Auth (usuário e senha).
- A ingestão de múltiplos endpoints é paralelizada com 2 workers por padrão.

## 🗃️ Esquema de Destino

- **Catálogo**: `ted_dev`
- **Schema**: `dev_guilherme_sobrinho`

**Nomenclatura das Tabelas**:
- `raw_database_<nome_tabela_sql>`
- `raw_api_<nome_endpoint_api>`

## 🚀 Como Executar o Pipeline

### 1. Clonar o Repositório no Databricks

1. No menu lateral, vá em **Repos**.
2. Clique em **Add Repo**.
3. Insira a URL do repositório Git e confirme.

### 2. Configurar os Secrets

Crie um Secret Scope no Databricks com o nome `guilherme_ferreira_checkpoint2_lh`.

#### Via CLI do Databricks (Recomendado):

```bash
# Criar o scope
databricks secrets create-scope --scope guilherme_ferreira_checkpoint2_lh

# Adicionar secrets para o banco de dados
databricks secrets put-secret guilherme_ferreira_checkpoint2_lh db_user --string-value "REDACTED"
databricks secrets put-secret guilherme_ferreira_checkpoint2_lh db_pass --string-value "REDACTED"
databricks secrets put-secret guilherme_ferreira_checkpoint2_lh db_host --string-value "REDACTED"
databricks secrets put-secret guilherme_ferreira_checkpoint2_lh db_port --string-value "REDACTED" 

# Adicionar secrets para a API
databricks secrets put-secret guilherme_ferreira_checkpoint2_lh api_user --string-value "REDACTED"
databricks secrets put-secret guilherme_ferreira_checkpoint2_lh api_password --string-value "REDACTED"
```

### 3. Executar o Notebook

1. Abra o notebook `databricks-database-ingestion`.
2. Anexe um cluster que tenha a dependência JDBC instalada (ver seção abaixo).
3. Execute todas as células do notebook (**Run All**). O pipeline fará a ingestão completa dos dados do banco e da API.

## 📦 Dependências

### Cluster Databricks

O cluster utilizado para executar o notebook deve ter a seguinte biblioteca Maven instalada:

- **Driver JDBC SQL Server**: `com.microsoft.sqlserver:mssql-jdbc:10.2.1.jre8`

### Bibliotecas Python

O código utiliza as seguintes bibliotecas, que geralmente já vêm pré-instaladas nos runtimes do Databricks:

- `pyspark`
- `requests`
- `json`
- `time`
- `datetime`
- `logging`
- `concurrent.futures`

## ❌ Opcional: Resetar o Ambiente

Caso queira apagar todas as tabelas ingeridas por este pipeline para uma reexecução limpa, você pode descomentar e executar a última célula do notebook. O código irá listar e apagar todas as tabelas dentro do schema `ted_dev.dev_guilherme_sobrinho`.

```python
schema = "dev_guilherme_sobrinho"

# Listar todas as tabelas do schema
tabelas = spark.sql(f"SHOW TABLES IN {schema}").filter("isTemporary = false").select("tableName").collect()

for t in tabelas:
    nome_tabela = t["tableName"]
    full_table_name = f"{schema}.{nome_tabela}"
    try:
        logger.info(f"Apagando tabela {full_table_name} ...")
        spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")
        logger.info(f"Tabela {full_table_name} apagada.")
    except Exception as e:
        logger.error(f"Erro ao apagar {full_table_name}: {e}")

```
