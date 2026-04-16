1. Storytelling 

A Glow & Co. agora quer se posicionar como uma marca de "Beleza Inteligente". O problema não é mais apenas entregar a caixa, mas sim garantir que o que está dentro dela entrega resultados. A diretoria de Inovação precisa deste painel para decidir qual será o próximo lançamento da marca baseado no que o mercado valoriza (ingredientes eficazes) e no que o consumidor está disposto a pagar.



2. Perguntas de negócio 

- "Qual é a relação entre o preço do produto e a presença de ingredientes 'premium'
Objetivo: identicar os igredientes que compoem os produtos mais caros para termos uma lista dos mais caros para o consumidor.

- "Quais são os ingredientes mais comuns nos produtos com melhor pontuação (Rank) para peles sensíveis em comparação a peles oleosas?"
Objetivo de Negócio: identificar a relação do rank de produtos mais vendidos com o tipo de pele para quais são direcionados, por exemplo, saber se produtos para peles sensíveis vendem melhor.

- "Existem produtos cujos nomes sugerem um ingrediente principal (ex: 'Aloe Vera Gel') mas que possuem esse ingrediente em baixa concentração na lista técnica?"
Objetivo de Negócio: Comparar o nome do ingrediente no produto com a sua concentração, e também ter a concentração média destes ingredientes.

3. Diagrama de arquitetura 


4. Como executar o projeto 

4.1 instalaçoes 



como instalar o airflow: 
referencias : https://www.youtube.com/watch?v=LwX9FFK9ojc&list=PLLNidqMOzeD5yXv9lDtBM-VJ5-1F-ZdXI&index=4
https://airflow.apache.org/docs/apache-airflow/stable/start.html


baixar o ubunto

reiniciar o computador e caso necessário ativar virtualizaçao da cpu  

<!-- no ubunto  -->
usar os comandos:

- cd desktop/ 
- mkdir airflow
- cd airflow/
- mkdir 2.6.1 #versao mais recente 
- sudo apt-get update
- python3 -- version # para ver sua versao do python, a minha é 3.12
- sudo apt install python3.12-venv
- cd 2.6.1/
- python3 -m venv .airflow
- source .airflow/bin/activate
- export AIRFLOW_HOME=$(pwd)
- export AIRFLOW_VERSION=2.6.1
- export PYTHON_VERSION ="$(python -- version | cut -d " " -f 2 | cut -d "." -f 1-2)"
- env | grep PYTHON
- export CONSTRAINT_URL = "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
- pip install "apache-airflow[async,postgres,google] ==${AIRFLOW_VERSION}" -- constraint "${CONSTRAINT_URL}"

<!-- AQUI COMEÇA A INSTALAÇAO -->

airflow standalone 
<!-- PARA INICIAR -->

abra o localhost:8080 no seu navegador e insira 
user admin 
senha RD6B4hsxfGdnRfcS


4. Fontes 

https://www.kaggle.com/datasets/amaboh/cosing-ingredients-inci-list?resource=download
https://www.kaggle.com/datasets/eward96/skincare-products-clean-dataset
https://www.kaggle.com/datasets/atharvasoundankar/cosmetics-and-skincare-product-sales-data-2022
https://www.kaggle.com/datasets/kingabzpro/cosmetics-datasets


5. Checklist de Auto-Avaliação para o Grupo 
• O storytelling descreve claramente o domínio e as perguntas de negócio. 
• O Airbyte/Script python carrega os dados para raw sem erros. 
• O Great Expectations valida a raw e gera relatório (HTML ou JSON). 
• O dbt produz staging, fatos e dimensões com surrogate keys. 
• A macro customizada é usada em pelo menos um modelo gold. 
• Os testes do dbt (genéricos) passam nos dados de exemplo. 
• O Airflow executa toda a DAG com sucesso ou trata falhas explicitamente. 
• O dashboard responde às perguntas de negócio. 
• O ambiente sobe com docker-compose up e o pipeline roda sem ajustes 
manuais. 
