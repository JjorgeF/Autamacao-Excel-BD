import pyodbc
import pandas as pd

# Informações de conexão
server = '*****'
database = '*****'
username = '*****'
password = '*****'
driver_name = '{ODBC Driver 17 for SQL Server}'
conexao_str = f'DRIVER={driver_name};SERVER={server};DATABASE={database};UID={username};PWD={password}'

# Dicionário para armazenar todos os DataFrames de todas as tabelas
# A estrutura será: {tabela_nome: [df_estrutura, df_descricoes, df_indices, df_fks]}
resultados_por_tabela = {}

try:
    # Tenta estabelecer a conexão
    conexao = pyodbc.connect(conexao_str)
    print("Conectado!!")

    # Passo 1: Obter a lista de todas as tabelas do banco de dados
    query_tabelas = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG = ? AND TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_NAME;"
    df_tabelas = pd.read_sql(query_tabelas, conexao, params=(database,))
    lista_tabelas = df_tabelas['TABLE_NAME'].tolist()
    print(f"Tabelas encontradas: {', '.join(lista_tabelas)}")

    # Queries SQL para extrair as informações. Note que agora TODAS usam @TB
    query_detalhes_tabela = """
        DECLARE @NmBanco AS VARCHAR(100)
        DECLARE @TB AS VARCHAR(50)
        SET @NmBanco = ? 
        SET @TB = ? 
        SELECT
            ROW_NUMBER() OVER(ORDER BY C.ORDINAL_POSITION) AS 'No.',
            C.COLUMN_NAME AS 'Nome da Coluna',
            ISNULL((SELECT 'X' FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KCU
                INNER JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC
                ON KCU.CONSTRAINT_NAME = TC.CONSTRAINT_NAME
                WHERE KCU.TABLE_NAME = C.TABLE_NAME
                AND KCU.COLUMN_NAME = C.COLUMN_NAME
                AND TC.CONSTRAINT_TYPE = 'PRIMARY KEY'), '-') AS 'PK',
            ISNULL((SELECT 'X' FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS AS RC
                INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KCU ON KCU.CONSTRAINT_NAME = RC.CONSTRAINT_NAME
                WHERE KCU.TABLE_NAME = C.TABLE_NAME
                    AND KCU.COLUMN_NAME = C.COLUMN_NAME), '-') AS 'Chave Estrangeira (FK)',
            IIF(C.IS_NULLABLE = 'YES', '-', 'X') AS 'M',
            CASE
                WHEN C.DATA_TYPE IN ('varchar', 'nvarchar', 'char', 'nchar') THEN C.DATA_TYPE + '(' + IIF(C.CHARACTER_MAXIMUM_LENGTH = -1, 'MAX', CAST(C.CHARACTER_MAXIMUM_LENGTH AS VARCHAR(10))) + ')'
                WHEN C.DATA_TYPE IN ('decimal', 'numeric') THEN C.DATA_TYPE + '(' + CAST(C.NUMERIC_PRECISION AS VARCHAR(10)) + ',' + CAST(C.NUMERIC_SCALE AS VARCHAR(10)) + ')'
                WHEN C.DATA_TYPE IN ('datetime2', 'datetimeoffset', 'time') THEN C.DATA_TYPE + '(' + CAST(C.DATETIME_PRECISION AS VARCHAR(10)) + ')'
                ELSE C.DATA_TYPE
            END AS 'Tipo de dado (data type)',
            CASE
                WHEN C.DATA_TYPE IN ('varchar', 'nvarchar', 'char', 'nchar') THEN 'tipo caractere'
                WHEN C.DATA_TYPE IN ('decimal', 'numeric', 'bigint', 'int', 'smallint', 'tinyint', 'float', 'real') THEN 'tipo numérico'
                WHEN C.DATA_TYPE IN ('datetime', 'datetime2', 'date', 'time', 'datetimeoffset') THEN 'tipo data'
                ELSE C.DATA_TYPE
            END AS 'Espécie do Tipo de Dado',
            'nativo do banco de dados' AS 'Origem do tipo de dado',
            ISNULL(C.COLUMN_DEFAULT, '-') AS 'Fórmula (caso aplicável)'
        FROM
            INFORMATION_SCHEMA.COLUMNS AS C
        WHERE
            C.TABLE_NAME = @TB
            AND C.TABLE_CATALOG = @NmBanco
        ORDER BY
            C.ORDINAL_POSITION;
    """
    
    # Consulta de descrições das colunas, agora filtrada por tabela
    query_descricoes = """
        DECLARE @TB AS VARCHAR(50)
        SET @TB = ?
        SELECT
            T.name AS 'Nome da Tabela',
            C.name AS 'Nome da Coluna',
            ISNULL (EP.value, '') AS 'Descrição'
        FROM
            sys.tables AS T
        INNER JOIN
            sys.columns AS C ON T.object_id = C.object_id
        LEFT JOIN
            sys.extended_properties AS EP ON EP.major_id = T.object_id
                AND EP.minor_id = C.column_id
                AND EP.name = 'MS_Description'
        WHERE
            T.name = @TB
        ORDER BY
            T.name,
            C.column_id;
    """

    # Consulta de índices, já com filtro por tabela
    query_detalhes_indices = """
        DECLARE @NmBanco AS VARCHAR(100)
        DECLARE @TB AS VARCHAR(50)
        SET @NmBanco = ?
        SET @TB = ?
        SELECT
            I.name AS 'Nome do Índice',
            COL_NAME(IC.object_id, IC.column_id) AS 'Nome da Coluna',
            CASE
                WHEN I.is_primary_key = 1 THEN 'Chave Primária'
                WHEN I.is_unique = 1 THEN 'Único'
                ELSE 'Não Único'
            END AS 'Tipo',
            I.type_desc AS 'Descrição do Tipo'
        FROM
            sys.indexes AS I
        INNER JOIN
            sys.index_columns AS IC ON I.object_id = IC.object_id AND I.index_id = IC.index_id
        WHERE
            I.object_id = OBJECT_ID(@TB)
        ORDER BY
            I.name, IC.index_column_id;
    """

    # Consulta de chaves estrangeiras, agora filtrada por tabela de origem
    query_fks = """
        DECLARE @TB AS VARCHAR(50)
        SET @TB = ?
        SELECT
            f.name AS 'Nome da Chave Estrangeira',
            OBJECT_NAME(f.parent_object_id) AS 'Tabela de Origem',
            COL_NAME(fc.parent_object_id, fc.parent_column_id) AS 'Coluna de Origem',
            OBJECT_NAME(f.referenced_object_id) AS 'Tabela de Destino',
            COL_NAME(fc.referenced_object_id, fc.referenced_column_id) AS 'Coluna de Destino'
        FROM
            sys.foreign_keys AS f
        INNER JOIN
            sys.foreign_key_columns AS fc ON f.object_id = fc.constraint_object_id
        WHERE
            OBJECT_NAME(f.parent_object_id) = @TB
        ORDER BY
            'Nome da Chave Estrangeira';
    """
    
    # Passo 2: Executar todas as consultas para cada tabela e armazenar os resultados
    for tabela in lista_tabelas:
        print(f"\nColetando informações da tabela: {tabela}")
        
        df_estrutura = pd.read_sql(query_detalhes_tabela, conexao, params=(database, tabela))
        df_descricoes = pd.read_sql(query_descricoes, conexao, params=(tabela,))
        df_indices = pd.read_sql(query_detalhes_indices, conexao, params=(database, tabela))
        df_fks = pd.read_sql(query_fks, conexao, params=(tabela,))
        
        # Armazena todos os DataFrames em uma lista para a tabela atual
        resultados_por_tabela[tabela] = {
            'estrutura': df_estrutura,
            'descricoes': df_descricoes,
            'indices': df_indices,
            'fks': df_fks
        }

        print(f"Informações de 4 consultas para a tabela '{tabela}' carregadas.")
        
    print("\nTodas as consultas foram executadas com sucesso!")

    # Passo 3: Salvar todos os DataFrames no arquivo Excel, em uma aba por tabela
    if resultados_por_tabela:
        with pd.ExcelWriter('detalhes_todas_tabelas.xlsx') as writer:
            for nome_tabela, dfs in resultados_por_tabela.items():
                current_row = 2 # Começa na linha 3 (B3)

                # Seção 1: Estrutura da Tabela
                pd.DataFrame([['--- ESTRUTURA DA TABELA ---']]).to_excel(writer, sheet_name=nome_tabela, header=False, index=False, startrow=current_row, startcol=1)
                dfs['estrutura'].to_excel(writer, sheet_name=nome_tabela, index=False, startrow=current_row + 1, startcol=1)
                current_row += len(dfs['estrutura']) + 3

                # Seção 2: Descrições das Colunas
                if not dfs['descricoes'].empty:
                    pd.DataFrame([['--- DESCRIÇÕES DAS COLUNAS ---']]).to_excel(writer, sheet_name=nome_tabela, header=False, index=False, startrow=current_row, startcol=1)
                    dfs['descricoes'].to_excel(writer, sheet_name=nome_tabela, index=False, startrow=current_row + 1, startcol=1)
                    current_row += len(dfs['descricoes']) + 3

                # Seção 3: Índices
                if not dfs['indices'].empty:
                    pd.DataFrame([['--- ÍNDICES ---']]).to_excel(writer, sheet_name=nome_tabela, header=False, index=False, startrow=current_row, startcol=1)
                    dfs['indices'].to_excel(writer, sheet_name=nome_tabela, index=False, startrow=current_row + 1, startcol=1)
                    current_row += len(dfs['indices']) + 3

                # Seção 4: Chaves Estrangeiras (FKs)
                if not dfs['fks'].empty:
                    pd.DataFrame([['--- CHAVES ESTRANGEIRAS (FKs) ---']]).to_excel(writer, sheet_name=nome_tabela, header=False, index=False, startrow=current_row, startcol=1)
                    dfs['fks'].to_excel(writer, sheet_name=nome_tabela, index=False, startrow=current_row + 1, startcol=1)
        
        print("\nArquivo Excel 'detalhes_todas_tabelas.xlsx' gerado com sucesso!")

except pyodbc.Error as ex:
    print(f"Erro na execução: {ex}")

finally:
    if 'conexao' in locals() and conexao:
        conexao.close()
        print("\nConexão com o banco de dados fechada.")