### Extração de Dados com Firecrawl e Análise com SQL AI Agent e DuckDB

#### Abra o terminal ou prompt de comando, navegue até a pasta com os arquivos e execute o comando abaixo para criar um ambiente virtual:

conda create --name <your_env_name_here> python=3.12

### Ative o ambiente:

conda activate <your_env_name_here> (ou: source activate <your_env_name_here>)

### Instale o pip e as dependências:

```
pip install -r requirements.txt
```

### Execute os scripts:

```
python dsa_web_scraping.py 
```

Neste primeito script você irá fazer o scraping de um site de livros, e com isso criar um database com os dados recolhidos. (neste repositório a database já se encontra populada, portando não precisa necessariamente correr este código).

```
streamlit run app.
```
Correndo este script, você irá abrir um browser onde poderá fazer perguntas sobre o banco de dados e o Agente AI vai responde-lo de maneira acertiva, usando SQL para consultar nossa database.
