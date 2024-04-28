# mba-impacta-dev-ops

Projeto disciplina de DataOps


## **Objetivos**:
- Ciclo de vida de projeto de dados
- Pipeline de dados
- Ferramentas básicas em dataops

```
dataops04
│   README.md
│   .gitignore
│   .github/workflows
|
└───python
|   └───scripts
│           │   ingestion.py
│           │   config.py
│           │   utils.py
│           │   metadado.xlsx
|   └───data
│           │   raw
|   |   Dockerfile
|   |   requirements.txt
|
└───mysql
|   └───db
│           │   create_database.sql
|   |   Dockerfile
|
└───execution_report
│   │   execution_report.docx
```

## **Detalhes**:
- ingestion.py
    - Define logging handlers para ingestão de logs em arquivo e streaming
    de logs em terminal de execução.
    - ingestion():
        - Promove tratamento de erros em request de dados via API.
        - Promove tratamento de erros quando do salvamento de dado bruto.
    - preparation():
        - Lê arquivo bruto para tratamento e instancia classe de saneamento de 
        dados, com tratamento de leitura de dados.
        - Renomeia as colunas do arquivo bruto de acordo com os nomes definidos 
        no arquivo de metadados.
        - Trata a tipagem dos registros de acordo com definição em metadados. 
        - Trata as colunas com valores str, convertendo todos os registros para
        lowercase e também converte special characters em ascii characters. 
        As colunas que devem ser tratadas estão representadas no arquivo de 
        metadados.
        - Salva os dados em banco relacional MySQL.
- config.py
    - Contém os caminhos dos diretórios de logs e dados raw, bem como de 
    metadados.
- utils.py
    - Classe que contém as funcões dedicadas ao pipeline de tratamento e 
    envio de dados para banco relacional.
- Dockerfile do microserviço da aplicação em python:
    - Usa imagem padrão do python e estabelece working directory com 
    todos os diretórios necessários para rodar a aplicação em container. Também
    faz a instalação das dependências de bibliotecas em python.
- Dockerfile do microserviço do banco relacional:
    - Usa imagem padrão do MySQL para criar tabela no banco relacional com 
    registro de entrada padrão de exemplo (Victoria Lambert).
- Arquivo docker-compose:
    - Orquestra os microserviços de python e do banco relacional. O 
    microserviço da aplicação em python também contém volumes em bind-mount 
    que permitem o monitoramento dos logs de execução gerados e também dos 
    arquivos da camada raw obtidos no diretório local fora do container.