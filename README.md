# openweather
Pipeline simples para extrair e transformar dados meteorológicos diários usando a API One Call (Day Summary) do OpenWeather.

O projeto automatiza três etapas principais:
- Identificar datas faltantes no Data Warehouse e gerar um arquivo de controle (`CSV`).
- Extrair resumos diários (day_summary) do OpenWeather para as datas faltantes.
- Transformar os arquivos JSON em um `CSV` consolidado com dados em Celsius e tipagem consistente.

As funções foram escritas em Python 3.12 e podem ser usadas localmente ou empacotadas para execução em AWS Lambda (há uma seção específica com instruções de layer).

## Visão Geral do Fluxo
- `src/missing_raw.py`: consulta a última data disponível no DW (tabela `raw.openweather__atb_daily`), calcula as datas faltantes até a última data possível e grava um arquivo CSV de controle (uma data por linha).
- `src/extraction.py`: lê o CSV de controle e faz requisições à API Day Summary do OpenWeather, salvando um JSON por dia (`day_summary_YYYY-MM-DD.json`).
- `src/transforming.py`: normaliza os JSONs para um único `CSV` (`all_dfs.csv`), convertendo temperaturas de Kelvin para Celsius e ajustando tipos de dados.

## Pré‑requisitos
- Python `>= 3.12`
- Dependências (gerenciadas por `pyproject.toml`):
  - `requests`, `pandas`, `psycopg2-binary`, `python-dotenv`
- Credenciais da API OpenWeather: variável `MY_API` no `.env`
- Acesso ao banco (PostgreSQL) para rodar a identificação de datas faltantes, se necessário

## Configuração Rápida
1. Crie um arquivo `.env` na raiz com a sua chave:
   - `MY_API=SEU_TOKEN_OPENWEATHER`
2. Instale as dependências (ex.: via `pip`):
   - `pip install -r <gerenciado pelo pyproject, use pip-tools/uv/poetry conforme seu fluxo>`
   - Alternativamente use sua ferramenta preferida (uv/poetry/pip-tools) para instalar com base no `pyproject.toml`.

## Execução

### 1) Identificar datas faltantes (opcional, requer DB)
Arquivo: `src/missing_raw.py`
- Responsável por gerar um `CSV` (ex.: `testing.csv`) com as datas a extrair.
- Conecta em PostgreSQL (ou utiliza um `PostgresHook` do Airflow) para consultar a última data existente e inferir as faltantes.
- Execução direta (exemplo local com `psycopg2`): ajuste os parâmetros de conexão no bloco `__main__` do arquivo.

Saída esperada: arquivo CSV com uma data por linha, no formato `YYYY-MM-DD`.

### 2) Extrair Day Summary por data
Arquivo: `src/extraction.py`
- Lê o arquivo de controle (`control_file`, ex.: `testing.csv`).
- Requisita a rota `https://api.openweathermap.org/data/3.0/onecall/day_summary` para cada data.
- Usa latitude/longitude fixas no código (ajuste conforme necessidade):
  - `lat = -23.137`
  - `lon = -46.5547861`
- Grava um JSON por dia no diretório de saída.

Exemplo de execução local:
- Garanta que o `.env` contém `MY_API`.
- Ajuste `output_path` e `control_file` no bloco `__main__` ou ao chamar `get_day_summary` via import.

### 3) Transformar JSONs em CSV consolidado
Arquivo: `src/transforming.py`
- Varre um diretório contendo os JSONs, normaliza as estruturas e converte temperaturas de Kelvin para Celsius.
- Gera `all_dfs.csv` no mesmo diretório de entrada.

Exemplo de uso via função:
- `from src.transforming import parsing_daily_weather`
- `parsing_daily_weather("<diretorio_com_jsons>")`

## Estrutura do Repositório
- `src/missing_raw.py`: identificação de datas faltantes e escrita do CSV de controle.
- `src/extraction.py`: extração dos day summaries pela API (usa `MY_API`).
- `src/transforming.py`: transformação/normalização e consolidação em `CSV`.
- `pyproject.toml`: metadados do projeto e dependências.
- `.env`: variável `MY_API` (não versionado), entre outras se desejar.
- `layer/`: pasta de suporte para empacotamento de dependências em AWS Lambda Layer.

## Variáveis de Ambiente
- `MY_API`: chave da API do OpenWeather.

## Observações
- A API Day Summary é disponibilizada no plano adequado do OpenWeather; verifique limites/quotas.
- Caso a extração retorne JSON vazio/erro, o log emitirá aviso e seguirá para o próximo arquivo.
- Ajuste `lat`/`lon` em `src/extraction.py` para sua região de interesse.

## Implementanção AWS Lambda
1. Criada a pasta `layer/`
2. Criada a pasta `layer/python`
3. Entrar na pasta `layer/python`
4. Instalar `requests`: `pip3 install requests -t .`
5. Voltar diretório `layer/`
6. Empacotar pasta `python` em um `.zip`: `zip -r python.zip python`
7. Fazer upload do `.zip` no Layer da Function

Sugestões adicionais:
- Para usar `pandas`/`psycopg2-binary` em Lambda, considere incluí-los no Layer ou usar imagens base com essas libs compiladas para o ambiente do Lambda.
- Configure `MY_API` como variável de ambiente da Function.
