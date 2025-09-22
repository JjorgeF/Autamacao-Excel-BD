import pyodbc
import pandas as pd
import xlsxwriter
import numpy as np
from datetime import datetime

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
    
    # NOVO: Query para obter todos os constraints da tabela
    query_constraints = """
        SELECT
            TC.CONSTRAINT_NAME AS 'Nome da Restrição',
            TC.CONSTRAINT_TYPE AS 'Tipo',
            CCU.COLUMN_NAME AS 'Nome da Coluna',
            'detalhes' AS 'Detalhes'
        FROM
            INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC
        JOIN
            INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE AS CCU
            ON TC.CONSTRAINT_NAME = CCU.CONSTRAINT_NAME
        WHERE
            TC.TABLE_NAME = ?;
    """

    # Passo 2: Executar todas as consultas para cada tabela e armazenar os resultados
    for tabela in lista_tabelas:
        print(f"\nColetando informações da tabela: {tabela}")
        
        df_estrutura = pd.read_sql(query_detalhes_tabela, conexao, params=(database, tabela))
        df_descricoes = pd.read_sql(query_descricoes, conexao, params=(tabela,))
        df_indices = pd.read_sql(query_detalhes_indices, conexao, params=(database, tabela))
        df_fks = pd.read_sql(query_fks, conexao, params=(tabela,))
        df_constraints = pd.read_sql(query_constraints, conexao, params=(tabela,))
        
        # Corrigido: Envolvendo o nome da tabela com colchetes [] para evitar erros com espaços
        query_linhas = f"SELECT COUNT(*) FROM [{tabela}];"
        df_linhas = pd.read_sql(query_linhas, conexao)
        num_linhas = df_linhas.iloc[0, 0]
        
        resultados_por_tabela[tabela] = {
            'estrutura': df_estrutura,
            'descricoes': df_descricoes,
            'indices': df_indices,
            'fks': df_fks,
            'num_linhas': num_linhas,
            'constraints': df_constraints
        }

        print(f"Informações de 5 consultas para a tabela '{tabela}' carregadas.")
        
    print("\nTodas as consultas foram executadas com sucesso!")

    # Passo 3: Salvar todos os DataFrames em uma única aba, um embaixo do outro, usando xlsxwriter
    if resultados_por_tabela:
        try:
            with pd.ExcelWriter('dicionario_todas_tabelas_unica_aba.xlsx', engine='xlsxwriter') as writer:
                workbook = writer.book
                worksheet = workbook.add_worksheet('Dicionário de Dados')
                writer.sheets['Dicionário de Dados'] = worksheet

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

                # NOVO: Formatos base (sem bordas)
                table_header_format_base = workbook.add_format({
                    'bold': True,
                    'bg_color': '#D9D9D9',
                    'border': 1
                })
                table_data_format_base = workbook.add_format({'border': 1})
                
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

                # NOVO: Formatos para o cabeçalho da tabela de detalhes
                header_sub_label_format = workbook.add_format({
                    'bold': True,
                    'bg_color': '#D9D9D9',
                    'font_color': 'black',
                    'border': 1
                })

                # títulos | 'Nome do banco de dados (dbname):' e o 'Nome da Tabela:' os dois em vermelho, com borda e negrito.
                header_label_format = workbook.add_format({
                    'bold': True,
                    'bg_color': '#D9D9D9',
                    'font_color': 'red',
                    'border': 1
                })

                header_value_format = workbook.add_format({
                    'border': 1,
                    'text_wrap': True
                })

                # NOVO: Formato para o título "Detalhes de Todas as Tabelas"
                title_format = workbook.add_format({
                    'bold': True,
                    'font_size': 14,
                    'bg_color': '#595959',
                    'font_color': 'white',
                    'align': 'center',
                    'valign': 'vcenter'
                })
                
                # NOVO: Formatos para as BORDAS AZUIS EXTERNAS
                blue_border = '#0070C0'
                
                # NOVO: Função para escrever o DataFrame e aplicar a borda externa
                def escrever_tabela_com_borda(worksheet, df, start_row, start_col):
                    num_rows = len(df)
                    num_cols = len(df.columns)

                    # Se o DataFrame estiver vazio, retorna um DataFrame com 3 linhas de '-'
                    # e 4 colunas para manter o padrão da imagem.
                    if num_rows == 0:
                        df = pd.DataFrame([['-'] * 4] * 3, columns=['', '', '', ''])
                        num_rows = len(df)
                        num_cols = len(df.columns)

                    # Escreve o cabeçalho
                    for col_num, value in enumerate(df.columns.values):
                        # Combina os formatos em um novo dicionário
                        format_dict = {
                            'bold': True, 'bg_color': '#D9D9D9', 'border': 1, 'border_color': 'black',
                            'top': 1, 'left': 1, 'right': 1, 'bottom': 1
                        }

                        if col_num == 0:
                            format_dict['left'] = 5
                            format_dict['border_color'] = blue_border
                        if col_num == num_cols - 1:
                            format_dict['right'] = 5
                            format_dict['border_color'] = blue_border
                        
                        format_dict['top'] = 5
                        format_dict['border_color'] = blue_border
                        
                        worksheet.write(start_row, start_col + col_num, value, workbook.add_format(format_dict))

                    # Escreve os dados
                    for row_num, row_data in enumerate(df.values):
                        for col_num, value in enumerate(row_data):
                            # Combina os formatos em um novo dicionário
                            format_dict = {
                                'border': 1, 'border_color': 'black',
                                'top': 1, 'left': 1, 'right': 1, 'bottom': 1
                            }
                            
                            if col_num == 0:
                                format_dict['left'] = 5
                                format_dict['border_color'] = blue_border
                            if col_num == num_cols - 1:
                                format_dict['right'] = 5
                                format_dict['border_color'] = blue_border
                            if row_num == num_rows - 1:
                                format_dict['bottom'] = 5
                                format_dict['border_color'] = blue_border

                            worksheet.write(start_row + 1 + row_num, start_col + col_num, value, workbook.add_format(format_dict))
                    
                    return start_row + 1 + num_rows + 1


                # ADICIONADO: Adiciona o título "Detalhes de Todas as Tabelas" na linha 2 da ÚNICA aba
                worksheet.merge_range('B2:L2', 'Detalhes de Todas as Tabelas', title_format)
                
                # Ajusta a largura das colunas
                for i in range(1, 10):
                    worksheet.set_column(i, i, 20)

                # Define a linha inicial para a primeira tabela
                current_row = 3

                # Loop para escrever cada tabela na mesma planilha
                # ADICIONADO: enumerate para obter o índice (numeração da tabela)
                for i, (nome_tabela, dfs) in enumerate(resultados_por_tabela.items(), 1):
                    # Seção do cabeçalho superior (Nome do Banco, Schema, etc.)
                    worksheet.write(current_row, 1, 'Nome do banco de dados (dbname):', header_label_format)
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
                    # ALTERADO: Usa o contador 'i' para criar a numeração da tabela
                    worksheet.merge_range(current_row, 1, current_row + 1, 2, f'Tabela {i:03}', header_format_blue) # título "Tabela 001", "Tabela 002", etc.
                    current_row += 2
                    
                    # Mescla o rótulo e escreve o valor
                    worksheet.merge_range(current_row, 1, current_row, 2, 'Nome da Tabela:', header_label_format)
                    worksheet.merge_range(current_row, 3, current_row, 7, nome_tabela, header_value_format)
                    current_row += 1
                    
                    # Mescla o rótulo e escreve o valor para 'Descrição'
                    worksheet.merge_range(current_row, 1, current_row, 2, 'Descrição:', header_sub_label_format)
                    worksheet.merge_range(current_row, 3, current_row, 7, 'Breve descrição do conteúdo da tabela. Breve descrição do conteúdo da tabela. Breve descrição do conteúdo da tabela.', header_value_format)
                    worksheet.set_row(current_row, 60)
                    current_row += 1
                    
                    # Mescla o rótulo e escreve o valor para 'Número de Colunas'
                    worksheet.merge_range(current_row, 1, current_row, 2, 'Número de Colunas:', header_sub_label_format)
                    worksheet.merge_range(current_row, 3, current_row, 7, len(dfs['estrutura']), header_value_format)
                    current_row += 1
                    
                    # Mescla o rótulo e escreve o valor para 'Número de Linhas'
                    worksheet.merge_range(current_row, 1, current_row, 2, 'Número de Linhas (atual):', header_sub_label_format)
                    worksheet.merge_range(current_row, 3, current_row, 7, dfs['num_linhas'], header_value_format)
                    current_row += 1

                    # Legenda
                    worksheet.write(current_row - 3, 9, 'PK = PRIMARY KEY (chave primária)', ) # 3 = a linha que ele vai ficar, podendo mudar de acordo com o que tem acima dela. 9 = a célula que ele vai ficar, no caso é a J
                    worksheet.write(current_row - 2, 9, 'FK = FOREIGN KEY (chave estrangeira)', )
                    worksheet.write(current_row - 1, 9, 'M = Mandatory (campo obrigatório)', )
                    
                    # Pula algumas linhas para a próxima seção
                    current_row += 2
                    
                    # Seção 1: Colunas
                    worksheet.write(current_row, 1, 'Colunas', header_format_gray)
                    current_row = escrever_tabela_com_borda(worksheet, dfs['estrutura'], current_row + 1, 1)

                    # NOVO: Seção 2: Descrições
                    worksheet.write(current_row, 1, 'Descrição das Colunas', header_format_gray)
                    current_row = escrever_tabela_com_borda(worksheet, dfs['descricoes'], current_row + 1, 1)

                    # NOVO: Seção 3: Índices
                    worksheet.write(current_row, 1, 'Índices (Indexes)', header_format_gray)
                    current_row = escrever_tabela_com_borda(worksheet, dfs['indices'], current_row + 1, 1)
                    
                    # NOVO: Seção 4: Chaves Estrangeiras (FKs)
                    worksheet.write(current_row, 1, 'Chaves Estrangeiras (FKs)', header_format_gray)
                    current_row = escrever_tabela_com_borda(worksheet, dfs['fks'], current_row + 1, 1)
                    
                    # NOVO: Seção 5: Restrições (Constraints)
                    worksheet.write(current_row, 1, 'Restrições (Constraints)', header_format_gray)
                    current_row = escrever_tabela_com_borda(worksheet, dfs['constraints'], current_row + 1, 1)

                    # Adiciona linhas de espaçamento entre as tabelas
                    current_row += 5

                print("\nArquivo Excel 'dicionario_todas_tabelas_unica_aba.xlsx' gerado com sucesso!")

        except Exception as e:
            print(f"Erro ao gerar o arquivo Excel: {e}")

except pyodbc.Error as ex:
    print(f"Erro na execução: {ex}")

finally:
    if 'conexao' in locals() and conexao:
        conexao.close()
        print("\nConexão com o banco de dados fechada.")
