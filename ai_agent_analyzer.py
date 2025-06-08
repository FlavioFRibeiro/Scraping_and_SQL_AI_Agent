# Projeto Especial - Parte 2 - Análise de Dados com SQL AI Agent e DuckDB

# Imports
import os                                                                   # Importa módulo para interação com o sistema operacional
import logging                                                              # Importa módulo para registrar logs
import duckdb                                                               # Importa biblioteca DuckDB para manipulação de bancos de dados
import datetime                                                             # Importa módulo para manipulação de datas e horários
from dotenv import load_dotenv                                              # Carrega variáveis de ambiente do arquivo .env
from langchain_community.utilities import SQLDatabase                       # Importa utilitário SQLDatabase do LangChain
from langchain_openai import ChatOpenAI                                     # Importa modelo de chat da OpenAI
from langchain_community.agent_toolkits.sql.base import create_sql_agent    # Importa função para criação de agente SQL
from langchain.agents import AgentExecutor                                  # Importa executor de agentes
import warnings
warnings.filterwarnings('ignore')

# Obtém a data e hora atual formatada como string
current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# Configuração do sistema de logs
logging.basicConfig(
    level = logging.INFO,                                                    # Define nível de log para INFO
    format = f'%(asctime)s - {current_time} - %(levelname)s - %(message)s',  # Define formato do log
    datefmt = '%Y-%m-%d %H:%M:%S'                                            # Define formato da data e hora
)

# Carrega as variáveis de ambiente do arquivo .env
load_dotenv()

# Verifica se a chave da API OpenAI está configurada no ambiente
if not os.getenv("OPENAI_API_KEY"):
    logging.error("Log - Erro Crítico: Variável de ambiente OPENAI_API_KEY não encontrada!")
    logging.error("Log - Certifique-se de que sua chave está no arquivo .env ou definida no sistema.")
    exit(1)  

# Define o nome do arquivo do banco de dados DuckDB
DUCKDB_FILE = "dados.duckdb"

# Define o nome da tabela a ser utilizada
TABLE_NAME = "books"

# Função principal que realiza a análise SQL usando um agente de IA
def analise_sql_agente_ia(question: str):

    # Registra o início da análise com o agente
    logging.info(f"Log - Iniciando Análise com SQL AI Agent Para a Pergunta: '{question}'")

    # Verifica se o arquivo do banco de dados existe
    if not os.path.exists(DUCKDB_FILE):
        logging.error(f"Log - Erro: Arquivo de banco de dados '{DUCKDB_FILE}' não encontrado.")
        logging.error("Log - Execute o script de scraping primeiro para criar e popular o banco de dados.")
        return  

    try:
        
        # Define URI para conexão com DuckDB
        db_uri = f"duckdb:///{os.path.abspath(DUCKDB_FILE)}"
        logging.info(f"Log - Conectando ao DB via LangChain com URI: {db_uri}")

        # Inicializa a conexão com o banco de dados via LangChain
        db = SQLDatabase.from_uri(db_uri, include_tables = [TABLE_NAME], sample_rows_in_table_info = 3)

        # Inicializa o modelo de linguagem (LLM) da OpenAI
        llm = ChatOpenAI(model = "gpt-4o-mini", temperature = 0.1)

        # Registra inicialização do modelo LLM
        logging.info(f"Log - LLM inicializado: {llm.model_name}")

        # Cria o agente executor SQL utilizando LangChain
        agent_executor: AgentExecutor = create_sql_agent(llm = llm,
                                                         db = db,
                                                         agent_type = "openai-tools",
                                                         verbose = True,
                                                         handle_parsing_errors = True)

        # Registra a criação do agente SQL
        logging.info("Log - Agente SQL criado com sucesso.")

        # Registra o início da execução do agente com a pergunta fornecida
        logging.info(f"Log - Invocando agente com a pergunta: '{question}'")

        # Invoca o agente com a pergunta e obtém a resposta
        response = agent_executor.invoke({"input": question})

        # Registra o término da execução do agente
        logging.info("Log - Agente concluiu a execução.")

        # Registra a resposta final do agente
        logging.info("Log - Resposta Final do Agente:")

        # Extrai a resposta final ou registra erro caso não exista
        final_answer = response.get('output', 'Erro: Chave "output" não encontrada na resposta.')

        # Imprime a pergunta e resposta do agente
        print(f"Pergunta: {question}")
        print(f"Resposta do Agente: {final_answer}")

        
        return final_answer

    except ImportError as ie:
        # Trata erros de importação de bibliotecas
        logging.error(f"Log - Erro de importação. Verifique as instalações: {ie}")
        logging.error("Log - Certifique-se de ter instalado as dependências.")

    except Exception as e:
        # Trata outros erros inesperados
        logging.error(f"Log - Ocorreu um erro inesperado durante a análise com o agente: {e}", exc_info = True)

    # Imprime separador visual
    print("==========================================")


# Execução principal do script
if __name__ == "__main__":

    # Registra o término do processo de análise
    logging.info("Processo de análise finalizado. Obrigado por usar o SQL AI Agent!")
    print("Processo de análise finalizado. Obrigado por usar o SQL AI Agent!")
    print("==========================================")




    