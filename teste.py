import pyodbc
import pandas as pd
import xlsxwriter
import numpy as np

# Informações de conexão
server = '*****'
database = '*****'
username = '*****'
password = '*****'
driver_name = '*****'
conexao_str = f'DRIVER={driver_name};SERVER={server};DATABASE={database};UID={username};PWD={password}'

# Dicionário para armazenar todos os DataFrames de todas as tabelas
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

    # Queries SQL
    query_detalhes_tabela = """
        DECLARE @NmBanco AS VARCHAR(100)
        DECLARE @TB AS VARCHAR(50)
        SET @NmBanco = ? 
        SET @TB = ? 
        SELECT
            ROW_NUMBER() OVER(ORDER BY C.ORDINAL_POSITION) AS 'No.',
            C.COLUMN_NAME AS 'Nome da Coluna',
            ISNULL((SELECT 'X' FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KCU INNER JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC ON KCU.CONSTRAINT_NAME = TC.CONSTRAINT_NAME WHERE KCU.TABLE_NAME = C.TABLE_NAME AND KCU.COLUMN_NAME = C.COLUMN_NAME AND TC.CONSTRAINT_TYPE = 'PRIMARY KEY'), '-') AS 'PK',
            ISNULL((SELECT 'X' FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS AS RC INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KCU ON KCU.CONSTRAINT_NAME = RC.CONSTRAINT_NAME WHERE KCU.TABLE_NAME = C.TABLE_NAME AND KCU.COLUMN_NAME = C.COLUMN_NAME), '-') AS 'Chave Estrangeira (FK)',
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

    # NOVO SELECT para as chaves estrangeiras, com a adição da cláusula WHERE
    query_fks = """
        DECLARE @TB AS VARCHAR(50)
        SET @TB = ?
        SELECT
            f.name AS 'Nome da Chave Estrangeira',
            OBJECT_NAME(f.parent_object_id) AS 'Referindo para',
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
        
        # Corrigido: Envolvendo o nome da tabela com colchetes [] para evitar erros com espaços
        query_linhas = f"SELECT COUNT(*) FROM [{tabela}];"
        df_linhas = pd.read_sql(query_linhas, conexao)
        num_linhas = df_linhas.iloc[0, 0]
        
        resultados_por_tabela[tabela] = {
            'estrutura': df_estrutura,
            'descricoes': df_descricoes,
            'indices': df_indices,
            'fks': df_fks,
            'num_linhas': num_linhas
        }

        print(f"Informações de 4 consultas para a tabela '{tabela}' carregadas.")
        
    print("\nTodas as consultas foram executadas com sucesso!")

    # Passo 3: Salvar todos os DataFrames no arquivo Excel, em uma aba por tabela, usando xlsxwriter
    if resultados_por_tabela:
        try:
            with pd.ExcelWriter('detalhes_todas_tabelas.xlsx', engine='xlsxwriter') as writer:
                workbook = writer.book

                # Define os formatos
                header_format_blue = workbook.add_format({
                    'bold': True,
                    'bg_color': '#0070C0',
                    'font_color': 'white',
                    'align': 'center',
                    'valign': 'vcenter'
                })
                header_format_gray = workbook.add_format({
                    'bold': True,
                    'bg_color': '#D9D9D9',
                    'font_color': 'black',
                    'align': 'left',
                    'valign': 'vcenter'
                })
                bold_format = workbook.add_format({'bold': True})
                red_format = workbook.add_format({'color': 'red'})
                
                # Formato para células mescladas de descrição com quebra de texto
                description_format = workbook.add_format({'text_wrap': True})

                # NOVO: Formato para as células de dados de tabela com bordas
                table_header_format = workbook.add_format({
                    'bold': True,
                    'border': 1,
                    'bg_color': '#D9D9D9' 
                })
                table_data_format = workbook.add_format({'border': 1})
                
                header_cell_format = workbook.add_format({
                    'bold': True,
                    'bg_color': '#D9D9D9',
                    'font_color': 'black',
                    'align': 'left',
                    'valign': 'vcenter',
                    'border': 1
                })
                value_cell_format = workbook.add_format({
                    'border': 1
                })

                # Função auxiliar para escrever o DataFrame com bordas
                def write_df_to_excel(worksheet, df, start_row, start_col):
                    # Escreve o cabeçalho
                    for col_num, value in enumerate(df.columns.values):
                        worksheet.write(start_row, start_col + col_num, value, table_header_format)
                    
                    # Escreve os dados
                    for row_num, row_data in enumerate(df.values):
                        for col_num, value in enumerate(row_data):
                            worksheet.write(start_row + 1 + row_num, start_col + col_num, value, table_data_format)

                for nome_tabela, dfs in resultados_por_tabela.items():
                    # Adiciona uma nova planilha para cada tabela
                    worksheet = workbook.add_worksheet(nome_tabela)
                    writer.sheets[nome_tabela] = worksheet 
                    
                    # Ajusta a largura das colunas
                    for i in range(1, 10):
                        worksheet.set_column(i, i, 20)
                    
                    current_row = 0 # Começa na linha 1 (índice 0)

                    # Seção do cabeçalho superior (Nome do Banco, Schema, etc.)
                    worksheet.write(current_row, 1, 'Nome do banco de dados (dbname):', header_cell_format)
                    worksheet.write(current_row, 2, database, value_cell_format)
                    current_row += 1
                    
                    worksheet.write(current_row, 1, 'Nome do Schema:', header_cell_format)
                    worksheet.write(current_row, 2, 'dbo', value_cell_format)
                    current_row += 1
                    
                    worksheet.write(current_row, 1, 'Código/sigla do banco de dados:', header_cell_format)
                    worksheet.write(current_row, 2, database, value_cell_format)
                    current_row += 1

                    worksheet.write(current_row, 1, 'SGBD:', header_cell_format)
                    worksheet.write(current_row, 2, 'Microsoft SQL Server', value_cell_format)
                    current_row += 1
                    
                    worksheet.write(current_row, 1, 'Quantidade de Tabelas:', header_cell_format)
                    worksheet.write(current_row, 2, len(lista_tabelas), value_cell_format)
                    current_row += 2 # pula 2 linhas para o próximo bloco

                    # Seção do Título da Tabela
                    worksheet.merge_range(current_row, 1, current_row + 1, 2, 'Tabela 001', header_format_blue)
                    current_row += 2
                    
                    worksheet.write(current_row, 1, 'Nome da Tabela:', bold_format)
                    worksheet.write(current_row, 2, nome_tabela, table_data_format)
                    current_row += 1
                    
                    worksheet.write(current_row, 1, 'Descrição:', bold_format)
                    worksheet.merge_range(current_row, 2, current_row, 7, 'Breve descrição do conteúdo da tabela. Breve descrição do conteúdo da tabela. Breve descrição do conteúdo da tabela.', description_format)
                    current_row += 1
                    
                    worksheet.write(current_row, 1, 'Número de Colunas:', bold_format)
                    worksheet.write(current_row, 2, len(dfs['estrutura']), table_data_format)
                    current_row += 1
                    
                    worksheet.write(current_row, 1, 'Número de Linhas (atual):', bold_format)
                    worksheet.write(current_row, 2, dfs['num_linhas'], table_data_format)
                    current_row += 1

                    # Legenda
                    worksheet.write(current_row - 3, 5, 'PK = PRIMARY KEY (chave primária)', red_format)
                    worksheet.write(current_row - 2, 5, 'FK = FOREIGN KEY (chave estrangeira)', red_format)
                    worksheet.write(current_row - 1, 5, 'M = Mandatory (campo obrigatório)', red_format)
                    
                    # Pula algumas linhas para a próxima seção
                    current_row += 2
                    
                    # Seção 1: Colunas
                    worksheet.write(current_row, 1, 'Colunas', header_format_gray)
                    write_df_to_excel(worksheet, dfs['estrutura'], current_row + 1, 1)
                    current_row += len(dfs['estrutura']) + 3

                    # Seção 2: Descrições
                    if not dfs['descricoes'].empty:
                        worksheet.write(current_row, 1, 'Descrição das Colunas', header_format_gray)
                        write_df_to_excel(worksheet, dfs['descricoes'], current_row + 1, 1)
                        current_row += len(dfs['descricoes']) + 3

                    # Seção 3: Índices
                    if not dfs['indices'].empty:
                        worksheet.write(current_row, 1, 'Índices (Indexes)', header_format_gray)
                        write_df_to_excel(worksheet, dfs['indices'], current_row + 1, 1)
                        current_row += len(dfs['indices']) + 3

                    # Seção 4: Chaves Estrangeiras (FKs)
                    if not dfs['fks'].empty:
                        worksheet.write(current_row, 1, 'Chaves Estrangeiras (FKs)', header_format_gray)
                        write_df_to_excel(worksheet, dfs['fks'], current_row + 1, 1)
            
            print("\nArquivo Excel 'detalhes_todas_tabelas.xlsx' gerado com sucesso!")

        except Exception as e:
            print(f"Erro ao gerar o arquivo Excel: {e}")

except pyodbc.Error as ex:
    print(f"Erro na execução: {ex}")

finally:
    if 'conexao' in locals() and conexao:
        conexao.close()
        print("\nConexão com o banco de dados fechada.")
