# 🌟 Glow & Co - Pipeline de Dados ELT

Pipeline completo de engenharia de dados para análise de produtos cosméticos e skincare.

## 🏗️ Arquitetura

```
Bronze (Extração)
    ↓
GX (Validação de Qualidade)
    ↓
dbt (Transformações)
    ↓
Silver (Análise Avançada)
    ↓
Gold (Dashboards de Negócio)
    ↓
Metabase (Provisioning de Visualizações)
```

## 🚀 Quick Start

### Pré-requisitos
- Docker & Docker Compose instalados
- PowerShell (Windows) ou Bash (Linux/Mac)

### Executar o Pipeline Completo

```bash
# 1. Inicie os serviços
docker-compose up -d

# 2. Aguarde PostgreSQL estar pronto (10-15 segundos)
docker-compose ps

# 3. Execute o pipeline
docker-compose exec g6 python main.py
```

### Acessar as Ferramentas

| Ferramenta | URL | Credenciais |
|------------|-----|-------------|
| **Metabase** | http://localhost:3000 | admin@metabase.local / metabase |
| **GX Docs** | http://localhost:8080 | - |
| **dbt Docs** | http://localhost:8181 | - |

## 🔧 Configuração

### Variáveis de Ambiente (.env)

```env
# PostgreSQL (dentro do Docker)
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_USER=glow
POSTGRES_PASSWORD=glow1234
POSTGRES_DB=glow_db

# Dados
BASE_DATA_PATH=./base
```

### Credenciais dos Serviços

| Serviço | Usuário | Senha |
|---------|---------|-------|
| PostgreSQL | `glow` | `glow1234` |
| Metabase | `admin@metabase.local` | `metabase` |

## 📊 Estrutura de Dados

### Camadas (Schemas)

- **`raw`** - Dados brutos extraídos (Bronze)
- **`silver`** - Análises avançadas e perguntas de negócio
- **`gold`** - Dashboards prontos para visualização
- **`public`** - Modelos dbt (staging, intermediate, marts)

### Tabelas Principais

#### Bronze (raw)
- `cosmetics_products` - Produtos cosméticos com ingredientes
- `skincare_products` - Produtos de skincare
- `sales_data` - Dados de vendas
- `cosing_ingredients` - Inventário de ingredientes

#### Silver
- `ingredient_pairs_premium` - Top pares de ingredientes
- `ingredient_trios_premium` - Top trios de ingredientes
- `ingredient_saturation_analysis` - Análise de saturação
- `controversial_ingredients_impact` - Impacto de alergênicos

#### Gold (Dashboards)
- `dashboard_combinacoes_ouro` - Combinações de ingredientes premium
- `dashboard_saturation_roi` - Análise de saturação vs ROI
- `dashboard_controverso_por_pele` - Impacto de alergênicos por tipo de pele
- `dashboard_premium_whitespace` - Oportunidades de mercado
- `dashboard_benchmark_competitivo` - Análise competitiva
- `dashboard_recomendacoes_inovacao` - Recomendações executivas

## 🐳 Containers Docker

| Container | Serviço | Porta |
|-----------|---------|-------|
| `g6` | App Python (Pipeline) | - |
| `glow_postgres` | PostgreSQL | 5432 |
| `metabase` | Metabase | 3000 |
| `static_gx` | GX Docs (Nginx) | 8080 |
| `dbt` | dbt CLI | - |
| `static_dbt` | dbt Docs (Nginx) | 8181 |

## 🔍 Diagnóstico

### Testar Conectividade

```bash
# Localmente (seu PC)
python diagnose.py

# Dentro do container Docker
docker-compose exec g6 python diagnose.py
```

### Verificar Logs

```bash
# Logs do container app (g6)
docker-compose logs -f g6

# Logs do PostgreSQL
docker-compose logs -f postgres

# Logs do Metabase
docker-compose logs -f metabase
```

### Executar Etapas Individualmente

```bash
# Apenas Bronze
docker-compose exec g6 python bronze/ingest.py

# Apenas Silver
docker-compose exec g6 python silver/silver.py

# Apenas Gold
docker-compose exec g6 python gold/gold.py

# Apenas Metabase Provisioning
docker-compose exec g6 python provisioning/metabase_provisioning.py
```

## 📝 Estrutura de Arquivos

```
.
├── main.py                          # Pipeline principal
├── diagnose.py                      # Script de diagnóstico
├── docker-compose.yml               # Configuração Docker
├── Dockerfile                       # Build da imagem Python
├── .env                             # Variáveis de ambiente
├── pyproject.toml                   # Dependências Python
│
├── base/                            # Dados de origem (CSV/XLS)
│   ├── cosmetics.csv
│   ├── skincare_products_clean.csv
│   └── cosmetics_sales_data.csv
│
├── bronze/                          # Camada de Extração (EL)
│   ├── ingest.py                    # Script de ingestão
│   └── raw.py                       # Manipulação de dados brutos
│
├── silver/                          # Camada de Análise Avançada
│   └── silver.py                    # Análises de negócio
│
├── gold/                            # Camada de Negócio Executiva
│   └── gold.py                      # Dashboards de visualização
│
├── glow_dbt/                        # Modelos dbt
│   ├── dbt_project.yml
│   ├── models/
│   │   ├── staging/                 # Dados limpos
│   │   ├── intermediate/            # Transformações intermediárias
│   │   └── marts/                   # Tabelas finais de análise
│   └── macros/                      # Funções reutilizáveis
│
├── provisioning/                    # Auto-provisioning de ferramentas
│   ├── metabase_provisioning.py     # Cria dashboards automaticamente
│   └── README.md
│
├── validation/                      # Validação de Qualidade (Great Expectations)
│   ├── gx_run.py
│   ├── gx_config.py
│   └── expectation_validation_*.py
│
├── profiles/                        # Configuração dbt
│   └── profiles.yml
│
├── sql/                             # Scripts SQL de inicialização
│   └── init.sql
│
└── frontend/                        # Persistência Metabase
    └── metabase_data/
        └── metabase/
            └── metabase.mv.db       # BD H2 do Metabase
```

## 🐛 Troubleshooting

### PostgreSQL não está respondendo

```bash
# Verifique a saúde
docker-compose ps postgres

# Verifique os logs
docker-compose logs postgres

# Reinicie
docker-compose restart postgres
```

### Metabase mostra erro de conexão

```bash
# Aguarde mais tempo para Metabase iniciar
docker-compose logs -f metabase

# Execute provisioning manualmente depois
docker-compose exec g6 python provisioning/metabase_provisioning.py
```

### Pipeline falha ao conectar ao banco

```bash
# Teste a conectividade
docker-compose exec g6 python diagnose.py

# Verifique a senha em .env
cat .env | grep POSTGRES
```

### dbt não encontra o banco

```bash
# Verifique profiles/profiles.yml
cat profiles/profiles.yml

# Teste dbt diretamente
docker-compose exec dbt dbt debug
```

## 📚 Mais Informações

- [dbt Documentation](glow_dbt/README.md)
- [Metabase Provisioning](provisioning/README.md)
- [Validation (GX)](validation/)

## 👥 Equipe

Projeto Glow & Co - Grupo 6

---

**Última atualização**: April 20, 2026
