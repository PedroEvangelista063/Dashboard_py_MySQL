import os
import pandas as pd
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv

# Carrega as variáveis do arquivo .env (não versionado)
load_dotenv()

# ==============================================================
# Caminhos dos seus arquivos CSV
# ==============================================================
path_tempo   = os.getenv('PATH_TEMPO',   'dim_tempo_mes.csv')
path_produto = os.getenv('PATH_PRODUTO', 'dim_produto.csv')
path_fato    = os.getenv('PATH_FATO',    'fato_sazonalidade.csv')

# ==============================================================
# Configuração de conexão com o TiDB Cloud
# ==============================================================
db_config = {
    'host': os.getenv('TIDB_HOST'),
    'port': int(os.getenv('TIDB_PORT', 4000)),
    'user': os.getenv('TIDB_USER'),
    'password': os.getenv('TIDB_PASSWORD'),
    'ssl_disabled': False,
    'ssl_verify_cert': False,
    'ssl_verify_identity': False,
}

# ==============================================================
# SQL para criar o banco de dados e as tabelas
# ==============================================================
CREATE_DATABASE = "CREATE DATABASE IF NOT EXISTS db_sazonalidade_agricola;"

CREATE_DIM_TEMPO = """
CREATE TABLE IF NOT EXISTS dim_tempo_mes (
    id_mes      INT PRIMARY KEY,
    nome_mes    VARCHAR(10) NOT NULL
);
"""

CREATE_DIM_PRODUTO = """
CREATE TABLE IF NOT EXISTS dim_produto (
    id_produto   INT PRIMARY KEY,
    categoria    VARCHAR(50) NOT NULL,
    nome_produto VARCHAR(100) NOT NULL
);
"""

CREATE_FATO_SAZONALIDADE = """
CREATE TABLE IF NOT EXISTS fato_sazonalidade (
    id_produto   INT NOT NULL,
    id_mes       INT NOT NULL,
    nivel_oferta CHAR(1) NOT NULL,
    PRIMARY KEY (id_produto, id_mes),
    CONSTRAINT fk_produto FOREIGN KEY (id_produto) REFERENCES dim_produto(id_produto),
    CONSTRAINT fk_mes     FOREIGN KEY (id_mes)     REFERENCES dim_tempo_mes(id_mes)
);
"""


def conectar_bd():
    """Conecta ao TiDB Cloud e retorna a conexão."""
    try:
        # Primeira conexão sem database para criar o banco
        conn = mysql.connector.connect(**db_config)
        if conn.is_connected():
            print("✅ Conectado ao TiDB Cloud com sucesso!")
            return conn
    except Error as e:
        print(f"❌ Erro ao conectar: {e}")
        return None


def criar_banco_e_tabelas(conn):
    """Cria o banco de dados e as tabelas se não existirem."""
    cursor = conn.cursor()
    try:
        # Cria o banco de dados
        print("\n📦 Criando banco de dados 'db_sazonalidade_agricola'...")
        cursor.execute(CREATE_DATABASE)

        # Seleciona o banco
        cursor.execute("USE db_sazonalidade_agricola;")

        # Cria as tabelas na ordem correta (dimensões primeiro, depois fato)
        print("📋 Criando tabela 'dim_tempo_mes'...")
        cursor.execute(CREATE_DIM_TEMPO)

        print("📋 Criando tabela 'dim_produto'...")
        cursor.execute(CREATE_DIM_PRODUTO)

        print("📋 Criando tabela 'fato_sazonalidade'...")
        cursor.execute(CREATE_FATO_SAZONALIDADE)

        conn.commit()
        print("✅ Banco e tabelas criados com sucesso!\n")
    except Error as e:
        print(f"❌ Erro ao criar banco/tabelas: {e}")
        conn.rollback()
    finally:
        cursor.close()


def inserir_dados(df, tabela, colunas_db, conn):
    """Insere os dados de um DataFrame em uma tabela do banco."""
    cursor = conn.cursor()
    placeholders = ', '.join(['%s'] * len(colunas_db))
    colunas_str = ', '.join(colunas_db)
    sql = f"INSERT IGNORE INTO {tabela} ({colunas_str}) VALUES ({placeholders})"

    # Converte o DataFrame em lista de tuplas
    dados = [tuple(x) for x in df.to_numpy()]

    try:
        cursor.executemany(sql, dados)
        conn.commit()
        print(f"   ✅ {cursor.rowcount} linhas inseridas em '{tabela}'.")
    except Error as e:
        print(f"   ❌ Erro ao inserir na tabela '{tabela}': {e}")
        conn.rollback()
    finally:
        cursor.close()


def main():
    # 1. Conectar ao TiDB
    conn = conectar_bd()
    if not conn:
        return

    # 2. Criar banco e tabelas
    criar_banco_e_tabelas(conn)

    # 3. Selecionar o banco para inserções
    cursor = conn.cursor()
    cursor.execute("USE db_sazonalidade_agricola;")
    cursor.close()

    print("🚀 Iniciando carga de dados (ETL)...\n")

    # ---- dim_tempo_mes ----
    print("📂 Carregando 'dim_tempo_mes.csv'...")
    df_tempo = pd.read_csv(path_tempo)
    # Colunas no CSV: id_mes, nome_mes  →  OK, batem com a tabela
    inserir_dados(df_tempo, 'dim_tempo_mes', ['id_mes', 'nome_mes'], conn)

    # ---- dim_produto ----
    print("📂 Carregando 'dim_produto.csv'...")
    df_produto = pd.read_csv(path_produto)
    # Colunas no CSV: id_produto, Categoria, Produto
    # Renomeia para bater com as colunas da tabela
    df_produto.rename(columns={
        'Categoria': 'categoria',
        'Produto': 'nome_produto'
    }, inplace=True)
    inserir_dados(df_produto, 'dim_produto', ['id_produto', 'categoria', 'nome_produto'], conn)

    # ---- fato_sazonalidade ----
    print("📂 Carregando 'fato_sazonalidade.csv'...")
    df_fato = pd.read_csv(path_fato)
    # Colunas no CSV: id_produto, id_mes, Nivel_Oferta
    # Renomeia para bater com as colunas da tabela
    df_fato.rename(columns={
        'Nivel_Oferta': 'nivel_oferta'
    }, inplace=True)
    inserir_dados(df_fato, 'fato_sazonalidade', ['id_produto', 'id_mes', 'nivel_oferta'], conn)

    # 4. Fechar conexão
    conn.close()
    print("\n🎉 ETL Finalizado! Dados prontos no TiDB para o Power BI.")


if __name__ == "__main__":
    main()
