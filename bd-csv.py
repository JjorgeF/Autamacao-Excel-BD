import pyodbc
import pandas as pd

# armazenei os dados em variáveis, por conta de erro na conexão com o banco
server = '*****' # aqui pode ser informado o nome do servidor ou o IP
database = 'AREAS' # informar o BD que deseja executar a extração
username = '*****' # o usuário
password = '*****' # a senha
driver_name = '{ODBC Driver 17 for SQL Server}' # informar o driver do banco | Procure pelo seu BD + Driver
conexao_str = f'DRIVER={driver_name};SERVER={server};DATABASE={database};UID={username};PWD={password}'

# dicionários para armazenar os DataFrames de cada tipo de consulta
# adicionar mais DataFrames para ocupar os outros SELECTs
resultados_colunas = {}
resultados_indices = {}

try:
    # Tenta estabelecer a conexão
    conexao = pyodbc.connect(conexao_str)
    print("Conectado!!")

    # Passo 1: Obter a lista de todas as tabelas do banco de dados
    query_tabelas = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG = ? AND TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_NAME;"
    df_tabelas = pd.read_sql(query_tabelas, conexao, params=(database,))
    
    lista_tabelas = df_tabelas['TABLE_NAME'].tolist()
    
    print(f"Tabelas encontradas: {', '.join(lista_tabelas)}")

    # Queries | Concatenar os SELECTs numa variavel
    query_detalhes_tabela = """
        DECLARE @NmBanco AS VARCHAR(100)
        DECLARE @TB AS VARCHAR(50)

        SET @NmBanco = ? 
        SET @TB = ? 

        SELECT
            ROW_NUMBER() OVER(ORDER BY C.ORDINAL_POSITION) AS 'No.',
            C.COLUMN_NAME AS 'Nome da Coluna',
            ISNULL((
                SELECT 'X'
                FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KCU
                INNER JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC
                    ON KCU.CONSTRAINT_NAME = TC.CONSTRAINT_NAME
                WHERE KCU.TABLE_NAME = C.TABLE_NAME
                AND KCU.COLUMN_NAME = C.COLUMN_NAME
                AND TC.CONSTRAINT_TYPE = 'PRIMARY KEY'
            ), '-') AS 'PK',
            ISNULL((
                SELECT 'X'
                FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS AS RC
                INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KCU ON KCU.CONSTRAINT_NAME = RC.CONSTRAINT_NAME
                WHERE KCU.TABLE_NAME = C.TABLE_NAME
                    AND KCU.COLUMN_NAME = C.COLUMN_NAME
            ), '-') AS 'Chave Estrangeira (FK)',
            IIF(C.IS_NULLABLE = 'YES', '-', 'X') AS 'M',
            CASE
                WHEN C.DATA_TYPE IN ('varchar', 'nvarchar', 'char', 'nchar')
                THEN C.DATA_TYPE + '(' + IIF(C.CHARACTER_MAXIMUM_LENGTH = -1, 'MAX', CAST(C.CHARACTER_MAXIMUM_LENGTH AS VARCHAR(10))) + ')'
                WHEN C.DATA_TYPE IN ('decimal', 'numeric')
                THEN C.DATA_TYPE + '(' + CAST(C.NUMERIC_PRECISION AS VARCHAR(10)) + ',' + CAST(C.NUMERIC_SCALE AS VARCHAR(10)) + ')'
                WHEN C.DATA_TYPE IN ('datetime2', 'datetimeoffset', 'time')
                THEN C.DATA_TYPE + '(' + CAST(C.DATETIME_PRECISION AS VARCHAR(10)) + ')'
                ELSE C.DATA_TYPE
            END AS 'Tipo de dado (data type)',
            CASE
                WHEN C.DATA_TYPE IN ('varchar', 'nvarchar', 'char', 'nchar')
                THEN 'tipo caractere'
                WHEN C.DATA_TYPE IN ('decimal', 'numeric', 'bigint', 'int', 'smallint', 'tinyint', 'float', 'real')
                THEN 'tipo numérico'
                WHEN C.DATA_TYPE IN ('datetime', 'datetime2', 'date', 'time', 'datetimeoffset')
                THEN 'tipo data'
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

    # Sua query para obter os índices
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
    
    for tabela in lista_tabelas:
        print(f"\nColetando informações da tabela: {tabela}")
        
        # Executa a primeira consulta e armazena o resultado
        df_colunas = pd.read_sql(query_detalhes_tabela, conexao, params=(database, tabela))
        resultados_colunas[tabela] = df_colunas
        
        # Executa a segunda consulta e armazena o resultado
        df_indices = pd.read_sql(query_detalhes_indices, conexao, params=(database, tabela))
        resultados_indices[tabela] = df_indices

        print(f"Informações de colunas e índices da tabela '{tabela}' carregadas.")
        
    print("\nTodas as consultas foram executadas com sucesso!")

    # Passo 4: Salvar todos os DataFrames em um único arquivo Excel, com uma aba por tabela
    if resultados_colunas:
        with pd.ExcelWriter('detalhes_todas_tabelas.xlsx') as writer:
            for nome_tabela, df_colunas in resultados_colunas.items():
                df_indices = resultados_indices.get(nome_tabela, pd.DataFrame())
                
                # Escreve o DataFrame de colunas na aba
                df_colunas.to_excel(writer, sheet_name=nome_tabela, index=False, startrow=0)
                
                # Se houver índices, adiciona-os logo abaixo
                if not df_indices.empty:
                    # Adiciona um título e uma linha de separação para clareza
                    linha_inicio_indices = len(df_colunas) + 2
                    pd.DataFrame([['--- ÍNDICES ---']]).to_excel(writer, sheet_name=nome_tabela, header=False, index=False, startrow=linha_inicio_indices)
                    
                    # Escreve o DataFrame de índices abaixo
                    df_indices.to_excel(writer, sheet_name=nome_tabela, index=False, startrow=linha_inicio_indices + 1)
        
        print("\nArquivo Excel 'detalhes_todas_tabelas.xlsx' gerado com sucesso!")

except pyodbc.Error as ex:
    print(f"Erro na execução: {ex}")

finally:
    if 'conexao' in locals() and conexao:
        conexao.close()

        print("\nConexão com o banco de dados fechada.")
