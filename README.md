📖 1. Storytelling: 

A Era da Beleza InteligenteA Glow & Co. está em transição para o modelo de "Beleza Inteligente". Em um mercado saturado, a diferenciação não vem apenas do marketing, mas da evidência científica e eficiência das fórmulas.

A Diretoria de Inovação enfrentava um "ponto cego": o desenvolvimento de novos produtos era baseado em tendências subjetivas. Este projeto nasceu para transformar dados brutos de formulações e vendas em insights estratégicos, permitindo que a marca identifique "Gaps" de mercado e crie produtos com alto Rank de satisfação e ROI otimizado.💡

---

2. Perguntas de Negócio (Business Questions)

Para guiar nossa análise, focamos em três pilares estratégicos:

Premiumness & Formulação: Qual a correlação entre o preço final e a presença de ativos nobres?
Objetivo: Listar ingredientes que justificam o ticket alto e mapear substitutos de custo-benefício.

Performance por Nicho (Pele Sensível vs. Oleosa): Quais ativos dominam os produtos com Rank > 4.5 para cada tipo de pele?
Objetivo: Identificar se a Glow & Co. deve focar em nichos específicos onde a concorrência é mal avaliada.

Transparência vs. Marketing (The "Hero" Ingredient): O ingrediente destacado no nome do produto (ex: "Gel de Aloe Vera") é realmente o protagonista na lista técnica (INCI)?
Objetivo: Mitigar riscos de reputação e garantir transparência na comunicação com o cliente.

---

🏗 3. Arquitetura do Pipeline (Medallion Architecture)O projeto utiliza o conceito de Medallion Architecture para garantir a qualidade do dado em cada etapa:

Ingestão (Bronze): Scripts Python extraem dados de fontes diversas (Kaggle/CSV) para o PostgreSQL (raw).

Qualidade (Great Expectations): Validação imediata da camada Raw para evitar "GIGO" (Garbage In, Garbage Out).

Transformação (Silver/dbt): Limpeza, padronização e criação de tabelas dimensionais e de fatos com surrogate keys.

Negócio (Gold): Modelagem de tabelas analíticas otimizadas para consumo direto nos dashboards.

Orquestração: Apache Airflow gerencia as dependências e o fluxo.
---

🚀 4. Como Executar o Projeto

4.1 Pré-requisitos

Docker e Docker Compose instalados.
Mínimo de 4GB de RAM dedicados ao Docker.

4.2 Instalação e Execução

Não é necessário instalar ferramentas individualmente no seu sistema local (como o Airflow pip), pois tudo rodará via Docker containers.

Clone o repositório:
Bashgit clone https://github.com/leticiacipriano-code/ProjetoFinal-Grupo6.git

Configure as variáveis de ambiente:
Crie um arquivo .env na raiz (use o .env.example como base).

Suba o ambiente:
Bashdocker-compose up --build -d
Este comando iniciará o PostgreSQL, Airflow, dbt, Metabase e Nginx.

Acompanhe o Pipeline:
Acesse o Airflow e ative a DAG glow_co_main_pipeline.

---

📊 5. Acessos Rápidos (Localhost)

| Ferramenta | URL |Credenciais| 
|------------|-----|-----------|
| Apache Airflow | localhost:8085 | admin / admin|
| Metabase (Dashboards) | localhost:3000 | Configuração inicial no primeiro acesso| 
| Data Docs (GX) | localhost:8080 | Relatórios de Qualidade | 
|  dbt Docs | localhost:8181 | Linhagem de Dados | 

---

📂 6. Fontes de Dados (Dataset)

Para reproduzir este projeto, baixe os arquivos nos links abaixo e coloque-os na pasta /data (se não estiverem versionados):

Cosmetics & Skincare Sales 2022Skincare Products Clean DatasetChemical Ingredients INCI List

---

✅ 7. Checklist de Autoavaliação

[ ] Storytelling: O domínio e as perguntas de negócio estão claros?
[ ] Ingestão: Os dados chegam na camada raw via Python?
[ ] Qualidade: O Great Expectations gera o relatório verde (sucesso)?
[ ] Transformação: O dbt gera modelos com surrogate keys e macros customizadas?
[ ] Orquestração: A DAG do Airflow executa sem falhas manuais?
[ ] Visualização: O dashboard responde se vale a pena lançar o novo produto?

---

🎨 8. Capturas de Tela (Demonstração)(Insira aqui os prints do seu projeto)Exemplo: Dashboard de Combinações de Ouro