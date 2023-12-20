# Lib para datas e horas
import datetime

# Lib para sistema opercional
import os
import platform

# Lib para conexão
import requests
import socket

# Lib para dados
import pandas as pd

# Lib para SQL
import sqlite3

# Ignorar Avisos
import warnings
warnings.filterwarnings('ignore')

import codecs
import os
import pyhdb

# Rotinas iniciais

# Nome rotinas
Id_Rotina = 2 
erros = []
Nome_Rotina = 'Extração de Dados - SAP Hana'

# Indentificar o usuario
def Identificando_Usuario():

    # Capturar o user
    Usuario = os.environ.get('USERNAME')

    # Capturar a maquina
    Maquina = platform.node()

    # Capturar o SO
    Sistema_Operacional = platform.platform()
    
    # Retorno da função
    return ( Usuario,  Maquina, Sistema_Operacional )

# Identificar o inicio
def Inicio_Rotina():
    
    # Data de Inicio
    Data_Inicio = datetime.datetime.today().date()
    
    # Hora de inicio
    Hora_Inicio = datetime.datetime.now()
    
    # REtorno
    return ( Data_Inicio, Hora_Inicio )

# Identificar o termino
def Termino_Rotina():
    
    # Data de Inicio
    Data_Fim = datetime.datetime.today().date()
    
    # Hora de inicio
    Hora_Fim = datetime.datetime.today().now()
    
    # REtorno
    return ( Data_Fim, Hora_Fim )

# Verificando conexão de internet
def Verificar_Conexao():

    # Conexão
    URL = 'https://www.google.com'

    # tempo de conexão
    Timeout = 5
    
    try:
        # Função para 
        requests.get( URL, timeout=Timeout )
        return True
    
    except:
        return False
    
# Identificando IP da Pessoa
def Indentificando_IP():
    
    try:
        # Identificando IP Local da Pessoa
        IP_Local = socket.gethostbyname( socket.gethostname() )
        return IP_Local
    
    except Exception as e:
        erro = f'Falha na identificação de IP: {str(e)}'
        erros.append(erro)
        print(erros)

# Gerando os parametros
try:
    Lista_Usuario = Identificando_Usuario()
except Exception as e:
    erro = f'Falha na identificação do Usuário: {str(e)}'
    erros.append(erro)
    print(erro)
Lista_Inicio = Inicio_Rotina()
try:
    Conexao_Internet = Verificar_Conexao()
    if not Conexao_Internet:
        raise Exception('Conexão com a internet indisponível')
except Exception as e:
    erro = f'Erro conexão com a internet: {str(e)}'
    erros.append(erro)
    print(erros)

try:
    Verificando_IP = Indentificando_IP()
except Exception as e:
    erro = f'Falha na identificação do IP: {str(e)}'
    erros.append(erro)
    print(erros)

# Colocar todos os erros que acontecer nosso processo
Erro_Operacional = ''


# ## Carregamento das Bibliotecas

try:
    import pandas as pd
    from dotenv import load_dotenv
    import os
    import codecs
    from datetime import date
except Exception as e:
    erro = f'Erro de importação de módulos: {str(e)}'
    erros.append(erro)
    print(erro)


# ## Configuração Conector Python

try:
    caminho_dot_env = r'CAMINMHO_DE_ARQUIVO_COM_USUARIO_E_SENHA_SAP_HANA'
    load_dotenv(caminho_dot_env)

    sap_hana_user = os.getenv("SAP_HANA_USER")
    sap_hana_password = os.getenv("SAP_HANA_PASSWORD")

# Establish connection to SAP HANA
    connection = pyhdb.connect(
    host='nome_da_base',
    port='porta(integer)',
    user=sap_hana_user,
    password=sap_hana_password
)

except Exception as e:
    erro = f'Erro na conexão com SAP Hana ou VPN desativada: {str(e)}'
    erros.append(erro)
    print(erro)


# ## Query - Serviços CRM CSS

try:
    # Executar consulta SQL e obter resultados em um DataFrame
    cursor = connection.cursor()
    query = """
     COLOQUE AQUI A SUA QUERY
    """
except Exception as e:
    erro = f'Erro na configuração da Query: {str(e)}'
    erros.append(erro)
    print(erros)


# CASO QUEIRA AUTOMATIZAR A EXTRAÇÃO DE OUTRAS BASES, COMO POR EXEMPLO AWS REDSHIFT SEGUE ABAIXO CONECTOR DE EXEMPLO
# try:
#     load_dotenv('RedShift.env')
    
#     redshift_user = os.getenv("AWS_REDSHIFT_USER")
#     redshift_password = os.getenv("AWS_REDSHIFT_PASSWORD")

    
#     # Configurações de conexão
#     redshift_host = "NOME_DO_HOST"
#     redshift_port = 'PORTA'
#     redshift_db = "NOME_BASE"
#     redshift_user = redshift_user
#     redshift_password = redshift_password
    
#     # Estabelecer a conexão com o Redshift
#     conn = psycopg2.connect(
#         host=redshift_host,
#         port=redshift_port,
#         dbname=redshift_db,
#         user=redshift_user,
#         password=redshift_password
#     )
    
#     # Executar consultas e comandos no Redshift
#     cursor = conn.cursor()
#     cursor.execute("SELECT * FROM tabela_teste")
#     result = cursor.fetchall()
#     for row in result:
#         print(row)
    
#     # Fechar a conexão
#     cursor.close()
#     conn.close()

except Exception as e:
    erro = f'Erro na conexão com serviço RedShift ou VPN desativada: {str(e)}'
    erros.append(erro)
    print(erro)

# ## Cursor e Resultado da Extração

try:
    cursor.execute(query)
    result = cursor.fetchall()
    df = pd.DataFrame(result, columns=[desc[0] for desc in cursor.description])
except Exception as e:
    erro = f'Erro na execução da Query: {str(e)}'
    erros.append(erro)
    print(erros)

try:
    data_ultima_extracao = date.today()
    df['Data da Última Extração'] = data_ultima_extracao
    print(df)
except Exception as e:
    erro = f'Erro no timestamp: {str(e)}'
    erros.append(erro)
    print(erros)

try:
    caminho_arquivo = r'CAMINHO_SALVAR_ARQUIVO_CSV'
    df.to_csv(caminho_arquivo, encoding='latin1')
    print(df)
except Exception as e:
    erro = f'Erro no carregamento do CSV: {str(e)}'
    erros.append(erro)
    print(erros)


try:
    caminho_arquivo = r'CAMINHO_SALVAR_ARQUIVO_EXCEL'
    df.to_excel(caminho_arquivo)
except Exception as e:
    erro = f'Erro no carregamento Excel: {str(e)}'
    erros.append(erro)
    print(erros)

try:
    # Termino da Rotina
    Lista_Fim = Termino_Rotina()
        
    import datetime
        
    def converter_tempo(tempo):
        horas = tempo.seconds // 3600
        minutos = (tempo.seconds % 3600) // 60
        segundos = tempo.seconds % 60
        
        return f'{horas:02d}:{minutos:02d}:{segundos:02d}'
        
    # Exemplo de uso com objetos datetime.timedelta
    tempo_execucao = Lista_Fim[1] - Lista_Inicio[1]
    Tempo_Execucao = converter_tempo(tempo_execucao)
        
    
    # Organização
    Dicionario = {
        'Id_Rotina' : Id_Rotina,
        'Nome_Rotina' : Nome_Rotina,
        'Usuario' : Lista_Usuario[0],
        'Maquina' : Lista_Usuario[1],
        'Sistema_Operacional' : Lista_Usuario[2],
        'Data_Inicio' : Lista_Inicio[0],
        'Horario_Inicio' : Lista_Inicio[1],
        'Teste_Conexao' : Conexao_Internet,
        'IP_Local' : Verificando_IP,
        'Data_Termino' : Lista_Fim[0],
        'Horario_Termino' : Lista_Fim[1],
        'Tempo_Execucao' : Tempo_Execucao,
        'Erro': erros[0] if erros else None
    }
        
        # Tab Log
        
    Tabela_Log = pd.DataFrame( Dicionario, index=[0] )
        
    # ---- Conexao SQL
        
    # Criar a conexão
        
    caminho_banco_de_dados = r'CAMINHO_BANCO_DE_DADOS_DB'
    Conexao = sqlite3.connect(caminho_banco_de_dados)
        
    # Apontar
    Cursor = Conexao.cursor()
        
    # Enviar as infos
    Tabela_Log.to_sql(
            
        # Nome Tabela
        'Tabela_Processamento',
            
        # Conexao
        Conexao,
            
        # Se a tabela existe
        if_exists='append',
            
        # Ignorar index
        index=False
            
    )
except Exception as e:
    erro = f'Erro na criação do log: {str(e)}'
    erros.append(erro)
    print(erros)


try:
    # Pandas
    df_log = pd.read_sql(
            
    # Query
    'SELECT * FROM Tabela_Processamento',
            
    # Conexao
    Conexao
    )
        
    print(df_log)
except Exception as e:
    erro = f'Erro de acesso da base de logs: {str(e)}'
    erros.append(erro)
    print(erros)


try:
    caminho_arquivo = r'CAMINHO_SALVAR_LOG'
    df_log.to_excel(caminho_arquivo)
    print(df_log)
except Exception as e:
    erro = f'Erro salvamento do log: {str(e)}'
    erros.append(erro)
    print(erros)





