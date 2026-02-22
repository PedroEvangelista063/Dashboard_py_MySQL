# Processo de ETL: Sazonalidade Agrícola no TiDB (AWS) para Power BI
Objetivo deste desafio : Integrar dados de arquivos CSV em um banco de dados em nuvem.

**1. Acertos Iniciais (Estrutura e Modelagem)**
- Dados limpos, filtrados e convertidos para Star Schema.
- Tabelas criadas com sucesso no TiDB via DDL no MySQL Workbench.

**2. Erros e Bloqueios (Tentativa via SQL Nativo)**
- **Erro 1064:** Incompatibilidade de sintaxe do TiDB no comando `LOAD DATA`. Corrigido alterando a cláusula de `IGNORE 1 ROWS` para `IGNORE 1 LINES`.
- **Erro 2068 (Bloqueio fatal):** O MySQL Workbench bloqueou a leitura dos arquivos no diretório local. Mesmo habilitando `SET GLOBAL local_infile=1` e `OPT_LOCAL_INFILE=1`, a política de segurança do cliente impediu a execução.

**3. Tentativa Descartada**
- Tentei *Table Data Import Wizard* aceditando que contornaria o erro, mas foi descartado por persistencia do erro.

**4. Solução Definitiva tentar via Python**
- **Abordagem:** Criação de script Python (`etl_tidb.py`) com `pandas` e `mysql-connector-python`.
- **Conexão:** SSL/TLS direto ao cluster TiDB (AWS) via `mysql.connector.connect()`, porta 4000.
- **Execução:** Leitura local dos CSVs e envio direto para a AWS com `cursor.executemany()` (Bulk Insert).
- **Resultado:** Eliminação total dos bloqueios do Workbench. 3948+ linhas inseridas com sucesso.

**5. Transformação dos Dados**
- No processo de transformação, as colunas com nome 'db_sazonalidade_agricola' foram apagadas.
- Nomes de colunas dos CSVs foram padronizados para o schema do banco (ex: Categoria → categoria, Produto → nome_produto, Nivel_Oferta → nivel_oferta).

**6. Status Final**
- ✅ Banco `db_sazonalidade_agricola` criado no TiDB Cloud (AWS).
- ✅ 3 tabelas carregadas: `dim_tempo_mes` (12), `dim_produto` (330), `fato_sazonalidade` (3948).
- ✅ Transformações aplicadas e dados prontos para criação do dashboard no Power BI.