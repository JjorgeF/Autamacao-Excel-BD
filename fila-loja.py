import pyodbc
import pandas as pd
import xlsxwriter
import numpy as np
from datetime import datetime

# Informações de conexão
# declarei as variáveis assumindo o valor a serem preenchidos para contornar erro ao acessar banco
server = '*****'
database = '*****'
username = '*****'
password = '*****'
driver_name = '*****'
conexao_str = f'DRIVER={driver_name};SERVER={server};DATABASE={database};UID={username};PWD={password}'


# dicionário para armazenar todos os DataFrames de todas as tabelas | facilita editar a perfumaria do excel depois
resultados_por_tabela = {}

try:
    # conecta no banco através do script
    conexao = pyodbc.connect(conexao_str)
    print("Conectado!! AEEEEE!!!")

    # -------------------------------------------------------------------------
    # MODIFICAÇÃO 1: Usando sys.tables para listar apenas tabelas de usuário (schema 'dbo')
    # -------------------------------------------------------------------------
    query_tabelas = """
        SELECT 
            t.name AS TABLE_NAME 
        FROM 
            sys.tables t
        INNER JOIN 
            sys.schemas s ON t.schema_id = s.schema_id
        WHERE 
            s.name = 'dbo'
        ORDER BY 
            t.name;
    """
    df_tabelas = pd.read_sql(query_tabelas, conexao)
    lista_tabelas = df_tabelas['TABLE_NAME'].tolist()
    
    # -------------------------------------------------------------------------
    # MODIFICAÇÃO 2: Filtro de segurança contra tabelas de sistema problemáticas (como 'captured_columns')
    # -------------------------------------------------------------------------
    tabelas_de_sistema_para_excluir = [
        'captured_columns', 
        'sysdiagrams', 
        'dtproperties', 
        # Adicione outras tabelas de sistema que possam surgir e causar erros
    ]
    lista_tabelas = [tabela for tabela in lista_tabelas if tabela not in tabelas_de_sistema_para_excluir]
    # -------------------------------------------------------------------------

    print(f"Tabelas encontradas: {', '.join(lista_tabelas)}")

    # queires do BD | Testei no BD primeiro e depois inseri cada SELECT numa variável

    # SELECT das Colunas (JÁ CORRIGIDO com CASE WHEN EXISTS)
    query_detalhes_tabela = """
        DECLARE @NmBanco AS VARCHAR(100)
        DECLARE @TB AS VARCHAR(50)
        SET @NmBanco = ? 
        SET @TB = ? 
        SELECT
            ROW_NUMBER() OVER(ORDER BY C.ORDINAL_POSITION) AS 'No.',
            C.COLUMN_NAME AS 'Nome da Coluna',
            CASE
                WHEN EXISTS (
                    SELECT 1 
                    FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KCU 
                    INNER JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC ON KCU.CONSTRAINT_NAME = TC.CONSTRAINT_NAME 
                    WHERE KCU.TABLE_NAME = C.TABLE_NAME 
                      AND KCU.COLUMN_NAME = C.COLUMN_NAME 
                      AND TC.CONSTRAINT_TYPE = 'PRIMARY KEY'
                ) THEN 'X'
                ELSE '-'
            END AS 'PK',
            CASE
                WHEN EXISTS (
                    SELECT 1 
                    FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS AS RC 
                    INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KCU ON KCU.CONSTRAINT_NAME = RC.CONSTRAINT_NAME 
                    WHERE KCU.TABLE_NAME = C.TABLE_NAME 
                      AND KCU.COLUMN_NAME = C.COLUMN_NAME
                ) THEN 'X'
                ELSE '-'
            END AS 'Chave Estrangeira (FK)',
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
    
    # SELECT da Descrição das Colunas
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
    # SELECT dos Indices
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
    # SELECT das Foreign Keys
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
    
    # SELECT dos Constraints
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
    
    # -------------------------------------------------------------------------
    # DEFINIÇÃO DO TEXTO DE SUBSTITUIÇÃO
    # -------------------------------------------------------------------------
    # Texto longo a ser encontrado (limpando o formato para comparação)
    TEXTO_LONGO_DEFAULT = """
/*------------------------------------------------------------*/
/* Criação de default */
/*------------------------------------------------------------*/

CREATE DEFAULT defZero AS 0"""
    
    # Normalizando a string para remover espaços e quebras de linha que a SQL pode introduzir
    TEXTO_A_PROCURAR_NORMALIZADO = ' '.join(TEXTO_LONGO_DEFAULT.split()).strip()
    
    # O valor final desejado
    NOVO_VALOR_DEFAULT = "CREATE DEFAULT defZero AS 0"
    
    # -------------------------------------------------------------------------

    # executar todas as consultas para cada tabela e armazenar os resultados
    for tabela in lista_tabelas: # percorre a lista de nomes de tabelas que foi coletada anteriormente.
        print(f"\nColetando informações da tabela: {tabela}")
        
        # Carrega os dados da estrutura (incluindo a coluna COLUMN_DEFAULT)
        df_estrutura = pd.read_sql(query_detalhes_tabela, conexao, params=(database, tabela))
        
        # =========================================================================
        # TRATAMENTO DA COLUNA 'Fórmula (caso aplicável)' (Solução para o formato)
        # =========================================================================
        
        coluna_default = 'Fórmula (caso aplicável)'

        # 1. Converte para string e normaliza removendo espaços em excesso para a comparação
        df_estrutura[coluna_default] = df_estrutura[coluna_default].astype(str).str.replace('\s+', ' ', regex=True).str.strip()

        # 2. Executa a substituição do texto longo pelo texto curto desejado
        df_estrutura[coluna_default] = df_estrutura[coluna_default].str.replace(
            TEXTO_A_PROCURAR_NORMALIZADO, 
            NOVO_VALOR_DEFAULT, 
            regex=False # Usa substituição literal, não regex, para a string longa
        )

        # 3. Limpeza adicional: Remove parênteses e colchetes comuns em defaults do SQL Server
        # Ex: '((0))' -> '0'; Ex: '([defZero])' -> 'defZero'
        df_estrutura[coluna_default] = df_estrutura[coluna_default].str.replace(r'^\(\((.*)\)\)$', r'\1', regex=True).str.strip()
        df_estrutura[coluna_default] = df_estrutura[coluna_default].str.replace(r'^\(\[(.*)\]\)$', r'\1', regex=True).str.strip()
        
        # O valor '-' deve ser restaurado caso a limpeza tenha removido parênteses de um valor NULL original que era '-'
        df_estrutura[coluna_default] = df_estrutura[coluna_default].replace(r'^\s*$', '-', regex=True)
        
        # =========================================================================
        
        df_descricoes = pd.read_sql(query_descricoes, conexao, params=(tabela,))
        df_indices = pd.read_sql(query_detalhes_indices, conexao, params=(database, tabela))
        df_fks = pd.read_sql(query_fks, conexao, params=(tabela,))
        df_constraints = pd.read_sql(query_constraints, conexao, params=(tabela,))
        
        # envolvendo o nome da tabela com colchetes [] para evitar erros com espaços
        query_linhas = f"SELECT COUNT(*) FROM [{tabela}];" # count de todas linhas da tabela
        df_linhas = pd.read_sql(query_linhas, conexao) # roda a query count no banco e realiza a conexão | armazenando na variável
        num_linhas = df_linhas.iloc[0, 0] # armazena o valor numérico puro nessa variável
        
        # dicionário para armazenar todos resultados de cima
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
            with pd.ExcelWriter('COHABSP - BD-teste - Anexo 2.xlsx', engine='xlsxwriter') as writer:
                workbook = writer.book
                worksheet = workbook.add_worksheet('Dicionário de Dados')
                writer.sheets['Dicionário de Dados'] = worksheet

                # define os formatos de vários partes

                # formatação do "Título 001"
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

                # formato para o título "Detalhes de Todas as Tabelas"
                title_format = workbook.add_format({
                    'bold': True,
                    'font_size': 14,
                    'bg_color': '#595959',
                    'font_color': 'white',
                    'align': 'center',
                    'valign': 'vcenter'
                })
                
                # função para escrever o DataFrame e aplicar a borda externa
                def escrever_tabela_sem_borda_azul(worksheet, df, start_row, start_col):
                    num_rows = len(df)
                    num_cols = len(df.columns)

                    if num_rows == 0:
                        # Preenche com 3 linhas de '-', mesmo que a consulta não retorne resultados
                        df = pd.DataFrame([['-'] * len(df.columns)] * 3, columns=df.columns)
                        num_rows = len(df)
                        
                    # escreve o cabeçalho
                    for col_num, value in enumerate(df.columns.values):
                        header_format = workbook.add_format({
                            'bold': True,
                            'bg_color': "#D9D9D9",
                            'border': 1,
                            'align': 'center'
                        })
                        
                        # --- MODIFICAÇÃO CHAVE 1: Ajuste de mesclagem do cabeçalho 'Descrição' ---
                        if value == 'Descrição' and 'No.' in df.columns: 
                            # Mescla de E (coluna 4) até J (coluna 9) (2 + col_num = 4)
                            # (9 é o índice da coluna J, se C é 2)
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
                            if df.columns.values[col_num] == 'Descrição' and 'No.' in df.columns:
                                # Mescla de E (col_num 2 + start_col 2 = 4) até J (coluna 9)
                                worksheet.merge_range(start_row + 1 + row_num, start_col + col_num, start_row + 1 + row_num, 8, value, wrap_format)
                                continue # Pula o write normal, pois já foi mesclado
                            
                            # Escreve os dados normalmente para outras colunas
                            worksheet.write(start_row + 1 + row_num, start_col + col_num, value, data_format)
                    
                    # Retorna a linha onde deve começar a próxima tabela.
                    return start_row + 1 + num_rows + 1

                # Título principal
                worksheet.merge_range('B2:L2', 'Detalhes de Todas as Tabelas', title_format)
                
                
                # --- MODIFICAÇÃO CHAVE 3: Ajuste da largura das colunas ---
                # Aumenta a largura das colunas E, F, G, H, I, J para acomodar o texto estendido da "Descrição"
                worksheet.set_column('C:C', 5) # Coluna 'No.'
                worksheet.set_column('D:D', 25) # Coluna 'Nome da Coluna'
                worksheet.set_column('E:J', 15) # Mescladas para 'Descrição'
                worksheet.set_column('K:L', 20) # Onde a legenda estava

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

                #print("\nArquivo Excel 'COHABSP - BD-002 - Anexo 2.xlsx' gerado com sucesso!")

        except Exception as e:
            print(f"Erro ao gerar o arquivo Excel: {e}")

except pyodbc.Error as ex:
    print(f"Erro na execução: {ex}")

finally:
    if 'conexao' in locals() and conexao:
        conexao.close()
        print("\nConexão com o banco de dados fechada.")
