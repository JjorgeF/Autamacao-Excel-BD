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


# dicionário para armazenar todos os DataFrames de todas as tabelas
resultados_por_tabela = {}

try:
    # Tenta estabelecer a conexão
    conexao = pyodbc.connect(conexao_str)
    print("Conectado!! AEEEEE!!!")

    # Passo 1: Obter a lista de todas as tabelas do banco de dados
    query_tabelas = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG = ? AND TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_NAME;"
    df_tabelas = pd.read_sql(query_tabelas, conexao, params=(database,))
    lista_tabelas = df_tabelas['TABLE_NAME'].tolist()
    print(f"Tabelas encontradas: {', '.join(lista_tabelas)}")

    # Queries SQL (mantidas as originais)
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
        DECLARE @NmBanco AS VARCHAR(100) -- Declarado!
        DECLARE @TB AS VARCHAR(50)

        SET @TB = ?

        SELECT
            -- ROW_NUMBER() OVER(ORDER BY C.ORDINAL_POSITION) AS 'No.', -- Coluna removida
            ROW_NUMBER() OVER(ORDER BY C.column_id) AS 'No.', -- Usando C.column_id para ordem
            C.name AS 'Nome da Coluna',
            ISNULL(EP.value, 'Nome autoexplicativo') AS 'Descrição'
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
            COL_NAME(IC.object_id, IC.column_id) AS 'Nome da(s) Coluna(s)',
            CASE
                WHEN I.is_primary_key = 1 THEN 'Chave Primária'
                WHEN I.is_unique = 1 THEN 'Único'
                ELSE 'Não Único'
            END AS 'Tipo'
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
        DECLARE @NmBanco AS VARCHAR(100)
        DECLARE @TB AS VARCHAR(50)
        SET @TB = ?
        SELECT
            f.name AS 'Nome',
            OBJECT_NAME(f.parent_object_id) AS 'Referindo de',
            COL_NAME(fc.parent_object_id, fc.parent_column_id) AS 'Coluna de Origem',
            OBJECT_NAME(f.referenced_object_id) AS 'Referindo para',
            COL_NAME(fc.referenced_object_id, fc.referenced_column_id) AS 'Coluna de Destino'
        FROM
            sys.foreign_keys AS f
        INNER JOIN
            sys.foreign_key_columns AS fc ON f.object_id = fc.constraint_object_id
        ORDER BY
            'Nome';
                    """
    
    # NOVO: Query para obter todos os constraints da tabela
    query_constraints = """
        SELECT
            TC.CONSTRAINT_TYPE AS 'Tipo',
            TC.CONSTRAINT_NAME AS 'Nome da Restrição',
            CCU.COLUMN_NAME AS 'Colunas',
            '-' AS 'Detalhes'
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

    ###### FORMATAÇÃO DAS CÉLULAS E LINHAS #####
    # Salvar todos os DataFrames em uma única aba, um embaixo do outro, usando xlsxwriter
    if resultados_por_tabela:
        try:
            with pd.ExcelWriter('COHABSP - BD-041 - Anexo 2.xlsx', engine='xlsxwriter') as writer:
                workbook = writer.book
                worksheet = workbook.add_worksheet('Dicionário de Dados')
                writer.sheets['Dicionário de Dados'] = worksheet

                # Define os formatos
                header_format_blue = workbook.add_format({
                    'bold': True,
                    'bg_color': '#0070C0',
                    'font_color': 'white',
                    'align': '', # estava dando conflito com o 'left' que o Xina pediu
                    'valign': 'vcenter',
                    'font_size': 16
                })
                # título de cada sessão (SELECTs) (Colunas, Descrições, Índices, FKs, Constraints)
                header_format_gray = workbook.add_format({
                    'bold': True,
                    'bg_color': "#FFFFFF",
                    'border': 1,
                    'border_color': "#ACACAC",
                    'font_color': 'black',
                    'align': 'left',
                    'valign': 'vcenter',
                    'italic': True, #itálico como o Xina pediu,
                    'font_size': 14
                })
                bold_format = workbook.add_format({'bold': True})
                red_format = workbook.add_format({'color': '#C00000'})
                
                # Formato para células mescladas de descrição com quebra de texto
                # O wrap_format será aplicado para a célula 'Descrição' na função de escrita
                wrap_format = workbook.add_format({'text_wrap': True, 'border': 1, 'align': 'left'}) # Incluído border e align left
                description_header_format = workbook.add_format({
                    'bold': True,
                    'bg_color': '#D9D9D9',
                    'border': 1,
                    'align': 'left', # Alinhamento à esquerda para o cabeçalho 'Descrição'
                })
                
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
                # células com os valores (ex: 'SGFWEB', 'dbo', 'Microsoft SQL Server', 'Quantidade de Tabelas')
                value_cell_format = workbook.add_format({
                    'border': 1,
                    'align': 'left'
                })

                # formatação do nome do banco e nome da tabela (em vermelho)
                bd_tabela_cell_format = workbook.add_format({
                    'font_color': '#C00000',
                    'border': 1,
                    })

                # formatos para o cabeçalho da tabela de detalhes
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
                    'font_color': '#C00000',
                    'border': 1
                })

                # valores | células que ficam do lado direito dos títulos, com borda e alinhamento à esquerda
                header_value_format = workbook.add_format({
                    'border': 1,
                    'text_wrap': True,
                    'align': 'left'
                })

                # Formato para o título "Detalhes de Todas as Tabelas"
                title_format = workbook.add_format({
                    'bold': True,
                    'font_size': 14,
                    'bg_color': '#595959',
                    'font_color': 'white',
                    'align': 'center',
                    'valign': 'vcenter'
                })
                
                # NOVO: Função para escrever o DataFrame e aplicar a borda externa
                def escrever_tabela_sem_borda_azul(worksheet, df, start_row, start_col):
                    num_rows = len(df)
                    num_cols = len(df.columns)

                    if num_rows == 0:
                        df = pd.DataFrame([['-'] * 4] * 3, columns=['', '', '', ''])
                        num_rows = len(df)
                        num_cols = len(df.columns)

                    # Escreve o cabeçalho
                    for col_num, value in enumerate(df.columns.values):
                        header_format = workbook.add_format({
                            'bold': True,
                            'bg_color': "#D9D9D9",
                            'border': 1,
                            'align': 'center'
                        })
                        
                        # --- MODIFICAÇÃO CHAVE 1: Ajuste de mesclagem do cabeçalho 'Descrição' ---
                        # Se for a coluna 'Descrição' e a tabela for 'descricoes', mescla o cabeçalho
                        if value == 'Descrição' and 'No.' in df.columns: # df_descricoes tem 'No.', 'Nome da Coluna', 'Descrição'
                            # Mescla da coluna de 'Descrição' (índice 2, que é a coluna D) até a coluna J (índice 9)
                            # O start_col para o dataframe é 2 (coluna C), a coluna 'Descrição' é a 3ª coluna (índice 2)
                            # Coluna 'No.' é C (col_num=0), 'Nome da Coluna' é D (col_num=1), 'Descrição' é E (col_num=2)
                            # Precisamos mesclar de E (start_col + col_num) até J (start_col + 7)
                            # start_col é 2. col_num para 'Descrição' é 2. A coluna E é o índice 4 (0=A, 1=B, 2=C, 3=D, 4=E)
                            # Como a tabela começa na coluna C (índice 2), 'Descrição' fica em E (índice 4).
                            # 'No.' é na coluna C (índice 2)
                            # 'Nome da Coluna' é na coluna D (índice 3)
                            # 'Descrição' é na coluna E (índice 4)
                            
                            # Para mesclar de E (col_num = 2 + start_col = 4) até J (col_num 9)
                            # O 'Nome da Coluna' é o 2º item (índice 1). A 'Descrição' é o 3º item (índice 2).
                            # Se a tabela começa em C (start_col=2), as colunas são:
                            # 0: 'No.' (C)
                            # 1: 'Nome da Coluna' (D)
                            # 2: 'Descrição' (E)
                            # A mesclagem vai de 'E' (col_num 2 + start_col 2 = 4) até 'J' (coluna 9)
                            worksheet.merge_range(start_row, start_col + col_num, start_row, 8, value, description_header_format)
                            continue # Pula o write normal, pois já foi mesclado
                        
                        # Escreve o cabeçalho normalmente para outras colunas ou tabelas
                        worksheet.write(start_row, start_col + col_num, value, header_format)

                    # Escreve os dados
                    for row_num, row_data in enumerate(df.values):
                        for col_num, value in enumerate(row_data):
                            data_format = workbook.add_format({
                                'border': 1,
                                'align': 'center'
                            })
                            
                            # --- MODIFICAÇÃO CHAVE 2: Ajuste de mesclagem dos dados da coluna 'Descrição' ---
                            # Se for a coluna 'Descrição' e a tabela for 'descricoes', mescla a célula de dados
                            if df.columns.values[col_num] == 'Descrição' and 'No.' in df.columns:
                                # Mescla de E (col_num 2 + start_col 2 = 4) até J (coluna 9)
                                worksheet.merge_range(start_row + 1 + row_num, start_col + col_num, start_row + 1 + row_num, 8, value, wrap_format)
                                continue # Pula o write normal, pois já foi mesclado
                            
                            # Escreve os dados normalmente para outras colunas
                            worksheet.write(start_row + 1 + row_num, start_col + col_num, value, data_format)
                    
                    # Retorna a linha onde deve começar a próxima tabela. 
                    # Se a coluna 'Descrição' foi mesclada (de E a J, ou seja, 8 colunas), 
                    # precisamos garantir que o próximo item comece na linha correta.
                    # A largura da tabela 'descricoes' é de 3 colunas (C, D, E). 
                    # A próxima linha é a mesma, mas se houver mesclagem, precisamos garantir que o cálculo 
                    # não dependa da largura da mesclagem para o cálculo de colunas
                    
                    # O cálculo do próximo 'current_row' está correto, pois depende da quantidade de linhas, não de colunas:
                    return start_row + 1 + num_rows + 1

                # Título principal
                worksheet.merge_range('B2:L2', 'Detalhes de Todas as Tabelas', title_format)
                
                
                # --- MODIFICAÇÃO CHAVE 3: Ajuste da largura das colunas ---
                # Aumenta a largura das colunas E, F, G, H, I, J para acomodar o texto estendido da "Descrição"
                worksheet.set_column('C:C', 5)     # Coluna 'No.'
                worksheet.set_column('D:D', 25)    # Coluna 'Nome da Coluna'
                worksheet.set_column('E:J', 15)    # Mescladas para 'Descrição'
                worksheet.set_column('K:L', 20)    # Onde a legenda estava

                # Define a linha inicial para a primeira tabela
                current_row = 3

                # Escreve as informações do banco de dados (sem alterações)
                worksheet.merge_range(current_row, 1, current_row, 2, ' Nome do banco de dados (dbname):', header_label_format)
                worksheet.merge_range(current_row, 3, current_row, 4, database, bd_tabela_cell_format)
                current_row += 1
                
                worksheet.merge_range(current_row, 1, current_row, 2, ' Nome do Schema:', header_cell_format)
                worksheet.merge_range(current_row, 3, current_row, 4, 'dbo', value_cell_format)
                current_row += 1
                
                worksheet.merge_range(current_row, 1, current_row, 2, ' Código/sigla do banco de dados:', header_cell_format)
                worksheet.merge_range(current_row, 3, current_row, 4, database, value_cell_format)
                current_row += 1

                worksheet.merge_range(current_row, 1, current_row, 2, ' SGBD:', header_cell_format)
                worksheet.merge_range(current_row, 3, current_row, 4, 'Microsoft SQL Server', value_cell_format)
                current_row += 1
                
                worksheet.merge_range(current_row, 1, current_row, 2, ' Quantidade de Tabelas:', header_cell_format)
                worksheet.merge_range(current_row, 3, current_row, 4, len(lista_tabelas), value_cell_format)
                current_row += 2 # pula 2 linhas para o próximo bloco

                # Loop para escrever cada tabela na mesma planilha
                for i, (nome_tabela, dfs) in enumerate(resultados_por_tabela.items(), 1):
                    # Seção do Título da Tabela
                    worksheet.merge_range(current_row, 1, current_row + 1, 2, f'  Tabela {i:03}', header_format_blue) # título "Tabela 001", "Tabela 002", etc.
                    current_row += 3
                    
                    # escrito 'Nome da Tabela' e na frente o nome da tabela em si (em vermelho)
                    worksheet.merge_range(current_row, 2, current_row, 3, ' Nome da Tabela:', header_label_format)
                    worksheet.merge_range(current_row, 4, current_row, 7, nome_tabela, bd_tabela_cell_format)
                    current_row += 1
                    
                    # Mescla o rótulo e escreve o valor para 'Descrição'
                    worksheet.merge_range(current_row, 2, current_row, 3, ' Descrição:', header_sub_label_format)
                    # Mescla de D (coluna 4) até G (coluna 7)
                    worksheet.merge_range(current_row, 4, current_row, 7, 'Nome autoexplicativo', header_value_format)
                    current_row += 1
                    
                    # Mescla o rótulo e escreve o valor para 'Número de Colunas'
                    worksheet.merge_range(current_row, 2, current_row, 3, ' Número de Colunas:', header_sub_label_format)
                    worksheet.merge_range(current_row, 4, current_row, 7, len(dfs['estrutura']), header_value_format)
                    current_row += 1
                    
                    # Mescla o rótulo e escreve o valor para 'Número de Linhas'
                    worksheet.merge_range(current_row, 2, current_row, 3, ' Número de Linhas (atual):', header_sub_label_format)
                    worksheet.merge_range(current_row, 4, current_row, 7, dfs['num_linhas'], header_value_format)
                    current_row += 1

                    # Legenda
                    # As células K e L (índices 10 e 11) estão livres, vamos usar J (índice 9)
                    worksheet.write(current_row - 4, 9, 'PK = PRIMARY KEY (chave primária)', ) # 9 = coluna J
                    worksheet.write(current_row - 3, 9, 'FK = FOREIGN KEY (chave estrangeira)', ) # 9 = coluna J
                    worksheet.write(current_row - 2, 9, 'M = Mandatory (campo obrigatório)', ) # 9 = coluna J
                    
                    # Pula algumas linhas para a próxima seção
                    current_row += 1
                    
                    # Seção 1: Colunas
                    worksheet.write(current_row, 2, 'Colunas', header_format_gray)
                    current_row = escrever_tabela_sem_borda_azul(worksheet, dfs['estrutura'], current_row + 1, 2)

                    # Seção 2: Descrições
                    worksheet.write(current_row, 2, 'Descrição das Colunas', header_format_gray)
                    # Usa a função modificada que faz a mesclagem da coluna 'Descrição'
                    current_row = escrever_tabela_sem_borda_azul(worksheet, dfs['descricoes'], current_row + 1, 2)

                    # Seção 3: Índices
                    worksheet.write(current_row, 2, 'Índices (Indexes)', header_format_gray)
                    current_row = escrever_tabela_sem_borda_azul(worksheet, dfs['indices'], current_row + 1, 2)
                    
                    # Seção 4: Chaves Estrangeiras (FKs)
                    worksheet.write(current_row, 2, 'Chaves Estrangeiras (Foreign Keys), referência "para"/"de"', header_format_gray)
                    current_row = escrever_tabela_sem_borda_azul(worksheet, dfs['fks'], current_row + 1, 2)
                    
                    # Seção 5: Restrições (Constraints)
                    worksheet.write(current_row, 2, 'Restrições (Constraints)', header_format_gray)
                    current_row = escrever_tabela_sem_borda_azul(worksheet, dfs['constraints'], current_row + 1, 2)

                    # espaço ao final da tabela constraints
                    current_row += 2

                print("\nArquivo Excel 'COHABSP - BD-041 - Anexo 2.xlsx' gerado com sucesso!")

        except Exception as e:
            print(f"Erro ao gerar o arquivo Excel: {e}")

except pyodbc.Error as ex:
    print(f"Erro na execução: {ex}")

finally:
    if 'conexao' in locals() and conexao:
        conexao.close()
        print("\nConexão com o banco de dados fechada.")
