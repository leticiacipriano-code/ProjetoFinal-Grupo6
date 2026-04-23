"""
=============================================================
Glow & Co. — Pipeline Gold — Camada de Negócio Executiva
=============================================================
Responsabilidade:
  1. Criar tabelas de negócio (gold layer) usando dados
     das tabelas marts do dbt (camada otimizada)
  2. Automatizar criação de dashboards no Metabase
  
  Tabelas geradas:
  1. Dashboard_Combinacoes_Ouro: Recomendações de ingredientes
  2. Dashboard_Saturation_ROI: Análise custo-benefício
  3. Dashboard_Controverso_Por_Pele: Impacto de alergênicos
  4. Dashboard_Premium_Whitespace: Oportunidades de mercado
  5. Dashboard_Benchmark_Competitivo: Análise comparativa
  6. Dashboard_Recomendacoes_Inovacao: Sugestões de produtos

Execução:
  python gold_metabase/gold.py

Variáveis de ambiente (via .env):
  POSTGRES_HOST, POSTGRES_PORT, POSTGRES_USER,
  POSTGRES_PASSWORD, POSTGRES_DB, METABASE_HOST,
  METABASE_USER, METABASE_PASSWORD
=============================================================
"""

from datetime import datetime, timezone
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import logging
import os
import sys
import requests
import json
import time


# ─────────────────────────────────────────────
# 1. Configuração de Logging
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("gold_metabase/gold.log", mode="a", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# 2. Carrega variáveis de ambiente
# ─────────────────────────────────────────────
load_dotenv()

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_USER = os.getenv("POSTGRES_USER", "glow")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "glow1234")
POSTGRES_DB = os.getenv("POSTGRES_DB", "glow_db")

DB_URL = (
    f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
    f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

# ─────────────────────────────────────────────
# 2b. Configuração Metabase
# ─────────────────────────────────────────────
METABASE_HOST = os.getenv("METABASE_HOST", "http://metabase:3000")
METABASE_USER = os.getenv("METABASE_USER", "admin@metabase.local")
METABASE_PASSWORD = os.getenv("METABASE_PASSWORD", "metabase")
METABASE_SCHEMA = "gold"


# ─────────────────────────────────────────────
# Classe MetabaseAPI - Integração com Metabase
# ─────────────────────────────────────────────

class MetabaseAPI:
    """API para criar dashboards automaticamente no Metabase."""
    
    def __init__(self, host, username, password):
        self.host = host
        self.base_url = f"{host}/api"
        self.auth = {"username": username, "password": password}
        self.session_id = None
        self.logger = logger

    def authenticate(self):
        """Autentica com o servidor Metabase."""
        try:
            resp = requests.post(
                f"{self.base_url}/session",
                json=self.auth,
                timeout=10
            )
            if resp.status_code == 200:
                self.session_id = resp.json()["id"]
                self.logger.info("✓ Metabase: Autenticado com sucesso")
                return True
        except Exception as e:
            self.logger.error(f"✗ Erro ao autenticar no Metabase: {e}")
        return False

    def get_headers(self):
        """Retorna headers para requisições autenticadas."""
        return {
            "X-Metabase-Session": self.session_id,
            "Content-Type": "application/json"
        }

    def get_db_id(self, db_name):
        """Obtém ID do banco de dados pelo nome."""
        try:
            resp = requests.get(
                f"{self.base_url}/database",
                headers=self.get_headers(),
                timeout=10
            )
            for db in resp.json():
                if db["name"] == db_name:
                    return db["id"]
        except Exception as e:
            self.logger.error(f"Erro ao obter DB ID: {e}")
        return None

    def get_table_id(self, db_id, schema, table_name):
        """Obtém ID da tabela pelo nome do schema e tabela."""
        try:
            resp = requests.get(
                f"{self.base_url}/database/{db_id}/metadata",
                headers=self.get_headers(),
                timeout=10
            )
            for table in resp.json().get("tables", []):
                if table["schema"] == schema and table["name"] == table_name:
                    return table["id"]
        except Exception as e:
            self.logger.error(f"Erro ao obter Table ID: {e}")
        return None

    def create_card(self, table_id, config):
        """Cria um card (gráfico) no Metabase."""
        try:
            payload = {
                "name": config["title"],
                "description": config.get("description", ""),
                "display": config["display_type"],
                "dataset_query": {
                    "database": None,
                    "type": "query",
                    "query": {"source-table": table_id}
                }
            }

            resp = requests.post(
                f"{self.base_url}/card",
                headers=self.get_headers(),
                json=payload,
                timeout=10
            )
            if resp.status_code == 200:
                card_data = resp.json()
                self.logger.info(f"✓ Card criado: {config['title']} (ID: {card_data['id']})")
                return card_data
            else:
                self.logger.error(f"✗ Erro ao criar card {config['title']}: {resp.text}")
        except Exception as e:
            self.logger.error(f"Erro ao criar card: {e}")
        return None

    def create_dashboard(self, name, description=""):
        """Cria um novo dashboard no Metabase."""
        try:
            payload = {
                "name": name,
                "description": description
            }
            resp = requests.post(
                f"{self.base_url}/dashboard",
                headers=self.get_headers(),
                json=payload,
                timeout=10
            )
            if resp.status_code == 200:
                dash_data = resp.json()
                self.logger.info(f"✓ Dashboard criado: {name} (ID: {dash_data['id']})")
                return dash_data
            else:
                self.logger.error(f"✗ Erro ao criar dashboard: {resp.text}")
        except Exception as e:
            self.logger.error(f"Erro ao criar dashboard: {e}")
        return None

    def add_card_to_dashboard(self, dash_id, card_id, row, col=0, size_x=12, size_y=8):
        """Adiciona um card a um dashboard."""
        try:
            payload = {
                "card_id": card_id,
                "row": row,
                "col": col,
                "size_x": size_x,
                "size_y": size_y
            }
            resp = requests.post(
                f"{self.base_url}/dashboard/{dash_id}/cards",
                headers=self.get_headers(),
                json=payload,
                timeout=10
            )
            if resp.status_code == 200:
                self.logger.info(f"✓ Card {card_id} adicionado ao dashboard {dash_id}")
                return True
            else:
                self.logger.error(f"✗ Erro ao adicionar card ao dashboard: {resp.text}")
        except Exception as e:
            self.logger.error(f"Erro ao adicionar card ao dashboard: {e}")
        return False

    def publish_dashboard(self, dash_id):
        """Publica um dashboard para torná-lo visível."""
        try:
            payload = {"canned_embedding_params": {}}
            resp = requests.put(
                f"{self.base_url}/dashboard/{dash_id}",
                headers=self.get_headers(),
                json=payload,
                timeout=10
            )
            if resp.status_code == 200:
                self.logger.info(f"✓ Dashboard {dash_id} publicado")
                return True
        except Exception as e:
            self.logger.error(f"Erro ao publicar dashboard: {e}")
        return False


# ─────────────────────────────────────────────
# 3. Funções Auxiliares
# ─────────────────────────────────────────────

def get_engine():
    """Cria conexão com o banco de dados PostgreSQL."""
    return create_engine(DB_URL, echo=False)


def create_gold_schema(engine):
    """Cria o schema gold se não existir."""
    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS gold"))
        conn.commit()
    logger.info("Schema 'gold' verificado/criado com sucesso")


def drop_table_if_exists(engine, table_name):
    """Dropa a tabela se existir."""
    with engine.connect() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS gold.{table_name} CASCADE"))
        conn.commit()
    logger.info(f"Tabela gold.{table_name} dropada (se existia)")


# ─────────────────────────────────────────────
# DASHBOARD 1: Combinações de Ouro (Top Performers)
# ─────────────────────────────────────────────

def create_dashboard_combinacoes_ouro(engine):
    """
    Combinações de ingredientes que impulsionam
    produtos premium com alto Rank.
    Usa dados de: marts.mart_pair_stats, marts.mart_ingredient_pairs
    Pronto para: Recomendações de formulação
    """
    
    sql = """
    CREATE TABLE gold.dashboard_combinacoes_ouro AS
    WITH top_pairs AS (
        SELECT
            mps.ing_1 || ' + ' || mps.ing_2 AS combinacao,
            mps.ing_1,
            mps.ing_2,
            'pair' AS tipo,
            mps.volume_produtos,
            mps.avg_rank,
            mps.price_tier,
            -- Calcula percentual de produtos com rank alto
            ROUND(
                100.0 * (mps.avg_rank / 5.0),
                2
            ) AS pct_high_rank
        FROM glow_marts.mart_pair_stats mps
        WHERE mps.avg_rank >= 4.0
            AND mps.volume_produtos >= 2
        ORDER BY mps.avg_rank DESC, mps.volume_produtos DESC
        LIMIT 20
    )
    SELECT
        ROW_NUMBER() OVER (ORDER BY avg_rank DESC, volume_produtos DESC) AS rank,
        combinacao,
        tipo,
        volume_produtos AS "Produtos com essa combinação",
        ROUND(avg_rank, 2) AS "Rank Médio",
        pct_high_rank AS "% de Qualidade Potencial",
        price_tier AS "Faixa de Preço",
        'Altamente recomendado para inovação' AS insight,
        NOW() AS data_atualizacao
    FROM top_pairs
    ORDER BY avg_rank DESC
    """
    
    with engine.connect() as conn:
        conn.execute(text(sql))
        conn.commit()
    
    logger.info("Tabela gold.dashboard_combinacoes_ouro criada com sucesso")


# ─────────────────────────────────────────────
# DASHBOARD 2: Análise ROI de Saturação
# ─────────────────────────────────────────────

def create_dashboard_saturation_roi(engine):
    """
    Curva de custo-benefício: quanto investir em
    ingredientes para máximo retorno em Rank.
    Usa dados de: marts.mart_unified_products
    Pronto para: Otimização de formulações
    """
    
    sql = """
    CREATE TABLE gold.dashboard_saturation_roi AS
    WITH products_analyzed AS (
        SELECT
            mup.product_name,
            mup.price,
            mup.rank::numeric AS rank_num,
            mup.price_tier,
            array_length(
                string_to_array(TRIM(mup.ingredients_list, '[]'), ', '),
                1
            ) AS num_ingredients,
            ROUND(
                mup.price / NULLIF(
                    array_length(
                        string_to_array(TRIM(mup.ingredients_list, '[]'), ', '),
                        1
                    ),
                    0
                ),
                2
            ) AS price_per_ingredient
        FROM glow_marts.mart_unified_products mup
        WHERE mup.ingredients_list IS NOT NULL
    ),
    
    saturation_by_count AS (
        SELECT
            num_ingredients,
            COUNT(*) AS total_products,
            ROUND(AVG(rank_num)::numeric, 2) AS avg_rank,
            ROUND(AVG(price)::numeric, 2) AS avg_price,
            ROUND(AVG(price_per_ingredient)::numeric, 2) AS avg_price_per_ingredient,
            COUNT(CASE WHEN rank_num >= 4.5 THEN 1 END) AS high_rank_count,
            ROUND(
                100.0 * COUNT(CASE WHEN rank_num >= 4.5 THEN 1 END) / COUNT(*)::numeric,
                2
            ) AS pct_high_rank,
            ROUND(MIN(rank_num)::numeric, 2) AS min_rank,
            ROUND(MAX(rank_num)::numeric, 2) AS max_rank
        FROM products_analyzed
        WHERE num_ingredients IS NOT NULL
            AND num_ingredients > 0
        GROUP BY num_ingredients
        ORDER BY num_ingredients
    )
    
    SELECT
        num_ingredients AS "Quantidade de Ingredientes",
        total_products AS "Total de Produtos",
        avg_rank AS "Rank Médio",
        avg_price AS "Preço Médio (R$)",
        avg_price_per_ingredient AS "Custo por Ingrediente (R$)",
        pct_high_rank AS "% Produtos com Rank Alto",
        ROUND(high_rank_count::numeric / total_products * 100, 1) AS "Taxa Sucesso (%)",
        CASE
            WHEN LAG(avg_rank) OVER (ORDER BY num_ingredients) IS NULL THEN 'baseline'
            WHEN avg_rank < LAG(avg_rank) OVER (ORDER BY num_ingredients) THEN 'declining'
            WHEN avg_rank > LAG(avg_rank) OVER (ORDER BY num_ingredients) THEN 'improving'
            ELSE 'stable'
        END AS "Tendência de Rank",
        CASE
            WHEN num_ingredients BETWEEN 8 AND 12 THEN 'Zona Ótima'
            WHEN num_ingredients < 8 THEN 'Subinvestido'
            WHEN num_ingredients > 15 THEN 'Saturado'
            ELSE 'Adequado'
        END AS "Zona de Formulação",
        CASE
            WHEN LAG(avg_rank) OVER (ORDER BY num_ingredients) IS NULL 
                THEN 'Primeira zona'
            WHEN (avg_rank - LAG(avg_rank) OVER (ORDER BY num_ingredients)) / 
                    NULLIF((avg_price - LAG(avg_price) OVER (ORDER BY num_ingredients)), 0) > 0.01
                THEN '✓ Investir'
            WHEN (avg_rank - LAG(avg_rank) OVER (ORDER BY num_ingredients)) IS NULL 
                THEN '✗ Evitar'
            ELSE '~ Manter'
        END AS "Recomendação",
        NOW() AS data_atualizacao
    FROM saturation_by_count
    ORDER BY num_ingredients
    """
    
    with engine.connect() as conn:
        conn.execute(text(sql))
        conn.commit()
    
    logger.info("Tabela gold.dashboard_saturation_roi criada com sucesso")


# ─────────────────────────────────────────────
# DASHBOARD 3: Impacto de Ingredientes Polêmicos
# ─────────────────────────────────────────────

def create_dashboard_controverso_por_pele(engine):
    """
    Impacto de ingredientes alergênicos/polêmicos
    em diferentes tipos de pele.
    Usa dados de: marts.mart_unified_products
    Pronto para: Análise de risco e oportunidade
    """
    
    sql = """
    CREATE TABLE gold.dashboard_controverso_por_pele AS
    WITH controversial_flags AS (
        SELECT
            mup.product_name,
            mup.rank::numeric AS rank_num,
            mup.price,
            CASE
                WHEN mup.sensitive::int = 1 THEN 'sensitive'
                WHEN mup.oily::int = 1 THEN 'oily'
                WHEN mup.dry::int = 1 THEN 'dry'
                ELSE 'combination'
            END AS skin_type,
            -- Detecta ingredientes polêmicos
            CASE
                WHEN (
                    LOWER(mup.ingredients_list) ~ 'sodium lauryl sulfate'
                    OR LOWER(mup.ingredients_list) ~ 'parabens'
                    OR LOWER(mup.ingredients_list) ~ 'sulfate'
                    OR LOWER(mup.ingredients_list) ~ 'alcohol denat'
                    OR LOWER(mup.ingredients_list) ~ 'fragrance'
                    OR LOWER(mup.ingredients_list) ~ 'parfum'
                    OR LOWER(mup.ingredients_list) ~ 'phenoxyethanol'
                    OR LOWER(mup.ingredients_list) ~ 'formaldehyde'
                ) THEN 1 ELSE 0
            END AS has_controversial_ingredient
        FROM glow_marts.mart_unified_products mup
        WHERE mup.ingredients_list IS NOT NULL
    ),
    
    impact_analysis AS (
        SELECT
            skin_type,
            has_controversial_ingredient,
            CASE WHEN has_controversial_ingredient = 1 THEN 'with_controversial' ELSE 'without_controversial' END AS ingredient_status,
            COUNT(*) AS total_products,
            ROUND(AVG(rank_num)::numeric, 2) AS avg_rank,
            ROUND(AVG(price)::numeric, 2) AS avg_price,
            COUNT(CASE WHEN rank_num >= 4.5 THEN 1 END) AS high_rank_count,
            ROUND(
                100.0 * COUNT(CASE WHEN rank_num >= 4.5 THEN 1 END) / COUNT(*)::numeric,
                2
            ) AS pct_high_rank
        FROM controversial_flags
        GROUP BY skin_type, has_controversial_ingredient
    )
    
    SELECT
        UPPER(skin_type) AS "Tipo de Pele",
        ingredient_status AS "Status do Ingrediente",
        total_products AS "Total de Produtos",
        avg_rank AS "Rank Médio",
        avg_price AS "Preço Médio (R$)",
        high_rank_count AS "Produtos com Rank Alto",
        pct_high_rank AS "% com Rank Alto",
        ROUND(
            avg_rank - MAX(CASE WHEN ingredient_status = 'without_controversial' THEN avg_rank END)
            OVER (PARTITION BY skin_type),
            2
        ) AS "Penalidade de Rank",
        ROUND(
            100.0 * (avg_rank - MAX(CASE WHEN ingredient_status = 'without_controversial' THEN avg_rank END)
            OVER (PARTITION BY skin_type)) /
            NULLIF(MAX(CASE WHEN ingredient_status = 'without_controversial' THEN avg_rank END)
            OVER (PARTITION BY skin_type), 0),
            2
        ) AS "Penalidade %",
        CASE
            WHEN skin_type = 'sensitive' AND 
                 (avg_rank - MAX(CASE WHEN ingredient_status = 'without_controversial' THEN avg_rank END)
                 OVER (PARTITION BY skin_type)) < -0.5 
                THEN 'CRÍTICO: Afeta muito peles sensíveis'
            WHEN skin_type = 'sensitive' AND 
                 (avg_rank - MAX(CASE WHEN ingredient_status = 'without_controversial' THEN avg_rank END)
                 OVER (PARTITION BY skin_type)) < -0.2 
                THEN 'Alto impacto em peles sensíveis'
            WHEN (avg_rank - MAX(CASE WHEN ingredient_status = 'without_controversial' THEN avg_rank END)
                 OVER (PARTITION BY skin_type)) < -0.1 
                THEN 'Impacto moderado'
            ELSE 'Impacto baixo'
        END AS "Alerta de Risco",
        NOW() AS data_atualizacao
    FROM impact_analysis
    ORDER BY 
        CASE WHEN skin_type = 'sensitive' THEN 0 ELSE 1 END,
        "Penalidade de Rank" ASC NULLS LAST
    """
    
    with engine.connect() as conn:
        conn.execute(text(sql))
        conn.commit()
    
    logger.info("Tabela gold.dashboard_controverso_por_pele criada com sucesso")


# ─────────────────────────────────────────────
# DASHBOARD 4: Whitespace Premium (Oportunidade)
# ─────────────────────────────────────────────

def create_dashboard_premium_whitespace(engine):
    """
    Combinações de ouro que NÃO existem em produtos
    de ticket médio (oportunidade de inovação).
    Usa dados de: marts.mart_pair_stats
    Pronto para: Identificar nichos inexplorados
    """
    
    sql = """
    CREATE TABLE gold.dashboard_premium_whitespace AS
    WITH high_performers AS (
        SELECT
            mps.ing_1,
            mps.ing_2,
            mps.price_tier,
            mps.avg_rank,
            mps.volume_produtos,
            COUNT(CASE WHEN mps.price_tier = 'Premium' THEN 1 END) 
                OVER (PARTITION BY mps.ing_1, mps.ing_2) AS premium_count,
            COUNT(CASE WHEN mps.price_tier = 'Mid-Ticket' THEN 1 END) 
                OVER (PARTITION BY mps.ing_1, mps.ing_2) AS midticket_count,
            CASE
                WHEN COUNT(CASE WHEN mps.price_tier = 'Premium' THEN 1 END) 
                     OVER (PARTITION BY mps.ing_1, mps.ing_2) > 0 
                     AND COUNT(CASE WHEN mps.price_tier = 'Mid-Ticket' THEN 1 END) 
                     OVER (PARTITION BY mps.ing_1, mps.ing_2) < 1 THEN 'Premium Exclusivo'
                ELSE 'Disponível'
            END AS segmento
        FROM glow_marts.mart_pair_stats mps
        WHERE mps.avg_rank >= 4.0
            AND mps.volume_produtos >= 2
    )
    
    SELECT
        ROW_NUMBER() OVER (
            PARTITION BY segmento 
            ORDER BY avg_rank DESC
        ) AS rank_no_segmento,
        ing_1 || ' + ' || ing_2 AS "Combinação Recomendada",
        segmento,
        ROUND(avg_rank, 2) AS "Rank Esperado",
        price_tier AS "Faixa de Preço (Referência)",
        volume_produtos AS "Produtos com esse par",
        CASE
            WHEN midticket_count < 1 THEN 'ALTA: Nicho inexplorado'
            WHEN midticket_count BETWEEN 1 AND 2 THEN 'MÉDIA: Pouca competição'
            ELSE 'BAIXA: Mercado competitivo'
        END AS "Potencial de Whitespace",
        'Aplicar essa combinação em produto de ticket médio pode capturar novo nicho' AS "Oportunidade",
        NOW() AS data_atualizacao
    FROM high_performers
    WHERE segmento = 'Premium Exclusivo'
    ORDER BY segmento, avg_rank DESC
    """
    
    with engine.connect() as conn:
        conn.execute(text(sql))
        conn.commit()
    
    logger.info("Tabela gold.dashboard_premium_whitespace criada com sucesso")


# ─────────────────────────────────────────────
# DASHBOARD 5: Benchmark Competitivo
# ─────────────────────────────────────────────

def create_dashboard_benchmark_competitivo(engine):
    """
    Análise comparativa de marcas e posicionamento
    de produtos no mercado.
    Usa dados de: marts.mart_unified_products
    Pronto para: Análise competitiva
    """
    
    sql = """
    CREATE TABLE gold.dashboard_benchmark_competitivo AS
    WITH produtos_base AS (
        SELECT
            mup.brand,
            COUNT(*) AS total_produtos,
            ROUND(AVG(mup.rank::numeric), 2) AS rank_medio_marca,
            ROUND(AVG(mup.price), 2) AS preco_medio_marca,
            COUNT(CASE WHEN mup.rank::numeric >= 4.5 THEN 1 END) AS produtos_premium_rank,
            ROUND(
                100.0 * COUNT(CASE WHEN mup.rank::numeric >= 4.5 THEN 1 END) / COUNT(*)::numeric,
                1
            ) AS pct_premium_rank,
            COUNT(CASE WHEN mup.price > 80 THEN 1 END) AS produtos_preco_premium,
            ROUND(MIN(mup.price), 2) AS preco_minimo,
            ROUND(MAX(mup.price), 2) AS preco_maximo
        FROM glow_marts.mart_unified_products mup
        WHERE mup.brand IS NOT NULL
            AND mup.brand != ''
        GROUP BY mup.brand
        HAVING COUNT(*) >= 2
    ),
    
    mercado_stats AS (
        SELECT
            ROUND(AVG(rank_medio_marca), 2) AS rank_mercado_medio,
            ROUND(AVG(preco_medio_marca), 2) AS preco_mercado_medio,
            ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY rank_medio_marca), 2) AS rank_q75,
            ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY preco_medio_marca), 2) AS preco_q75
        FROM produtos_base
    )
    
    SELECT
        pb.brand AS "Marca",
        pb.total_produtos AS "Total de Produtos",
        pb.rank_medio_marca AS "Rank Médio",
        CASE
            WHEN pb.rank_medio_marca >= ms.rank_q75 THEN '⭐⭐⭐ Premium'
            WHEN pb.rank_medio_marca >= ms.rank_mercado_medio THEN '⭐⭐ Acima da média'
            ELSE '⭐ Abaixo da média'
        END AS "Posição de Qualidade",
        pb.preco_medio_marca AS "Preço Médio (R$)",
        CASE
            WHEN pb.preco_medio_marca >= ms.preco_q75 THEN 'Premium'
            WHEN pb.preco_medio_marca >= ms.preco_mercado_medio THEN 'Mid-ticket'
            ELSE 'Economy'
        END AS "Segmento de Preço",
        pb.pct_premium_rank AS "% Produtos com Rank Alto",
        pb.preco_minimo AS "Preço Mínimo (R$)",
        pb.preco_maximo AS "Preço Máximo (R$)",
        CASE
            WHEN pb.rank_medio_marca >= ms.rank_q75 AND pb.preco_medio_marca >= ms.preco_q75 
                THEN 'Líder: Qualidade + Premium'
            WHEN pb.rank_medio_marca >= ms.rank_mercado_medio AND pb.pct_premium_rank >= 50 
                THEN 'Strong Performer'
            WHEN pb.preco_medio_marca < ms.preco_mercado_medio AND pb.rank_medio_marca >= ms.rank_mercado_medio
                THEN 'Value Leader'
            ELSE 'Nicho/Emergente'
        END AS "Estratégia de Posicionamento",
        NOW() AS data_atualizacao
    FROM produtos_base pb
    CROSS JOIN mercado_stats ms
    ORDER BY pb.rank_medio_marca DESC, pb.preco_medio_marca DESC
    """
    
    with engine.connect() as conn:
        conn.execute(text(sql))
        conn.commit()
    
    logger.info("Tabela gold.dashboard_benchmark_competitivo criada com sucesso")


# ─────────────────────────────────────────────
# DASHBOARD 6: Recomendações de Inovação
# ─────────────────────────────────────────────

def create_dashboard_recomendacoes_inovacao(engine):
    """
    Síntese executiva: Recomendações de novos
    produtos baseadas em dados analíticos.
    Usa dados de: todas as tabelas gold criadas
    Pronto para: Decisões de negócio (C-suite)
    """
    
    sql = """
    CREATE TABLE gold.dashboard_recomendacoes_inovacao AS
    WITH recomendacoes_compiladas AS (
        -- REC 1: Combinações de ouro em segmento mid-ticket
        SELECT
            'Aplicar Combinação de Ouro em Mid-ticket' AS tipo_recomendacao,
            1 AS prioridade,
            'ALTA' AS impacto_esperado,
            'Implementar em 90 dias' AS timeline,
            'Usar combinações que têm rank 4.0+ em Premium, mas ausentes em Mid-ticket' AS acao,
            'Capturar whitespace com produtos de R$ 40-60 que atingem Rank 4.0+' AS resultado_esperado,
            (SELECT COUNT(*) FROM gold.dashboard_combinacoes_ouro)::text AS casos_suporte
        
        UNION ALL
        
        -- REC 2: Ponto ótimo de ingredientes
        SELECT
            'Otimizar Quantidade de Ingredientes',
            2,
            'ALTA',
            'Implementar em 60 dias',
            'Limitar formulações a 10-12 ingredientes (zona ótima) para maximizar ROI',
            'Reduzir custo de produção enquanto mantém/melhora Rank',
            (SELECT COUNT(*) FROM gold.dashboard_saturation_roi 
             WHERE "Zona de Formulação" = 'Zona Ótima')::text
        
        UNION ALL
        
        -- REC 3: Cuidado com alergênicos em pele sensível
        SELECT
            'Evitar Ingredientes Polêmicos em Pele Sensível',
            1,
            'CRÍTICA',
            'Imediato',
            'Reformular produtos para pele sensível sem ingredientes polêmicos',
            'Aumentar Rank em 0.3-0.5 pontos, melhorando retenção de clientes',
            (SELECT COUNT(*) FROM gold.dashboard_controverso_por_pele 
             WHERE "Tipo de Pele" = 'SENSITIVE' 
             AND "Alerta de Risco" LIKE 'CRÍTICO%')::text
        
        UNION ALL
        
        -- REC 4: Whitespace em Mid-ticket
        SELECT
            'Explorar Premium Whitespace em Mid-Ticket',
            2,
            'ALTA',
            'Análise em 45 dias',
            'Lançar produtos em tiered pricing usando combinações premium',
            'Expandir market share em segmento crescente',
            (SELECT COUNT(*) FROM gold.dashboard_premium_whitespace)::text
        
        UNION ALL
        
        -- REC 5: Posicionamento competitivo
        SELECT
            'Revisar Posicionamento vs Concorrência',
            3,
            'MÉDIA',
            'Análise em 30 dias',
            'Comparar estratégia de preço/qualidade com líderes de mercado',
            'Definir estratégia clara: Premium, Value, ou Nicho',
            (SELECT COUNT(*) FROM gold.dashboard_benchmark_competitivo 
             WHERE "Estratégia de Posicionamento" = 'Líder: Qualidade + Premium')::text
    )
    
    SELECT
        ROW_NUMBER() OVER (ORDER BY prioridade) AS id,
        tipo_recomendacao AS "Recomendação",
        prioridade AS "Prioridade (1=Alta, 3=Baixa)",
        impacto_esperado AS "Impacto Esperado",
        timeline AS "Timeline",
        acao AS "Ação Recomendada",
        resultado_esperado AS "Resultado Esperado",
        casos_suporte || ' casos analisados' AS "Base de Dados",
        NOW() AS data_atualizacao
    FROM recomendacoes_compiladas
    ORDER BY prioridade
    """
    
    with engine.connect() as conn:
        conn.execute(text(sql))
        conn.commit()
    
    logger.info("Tabela gold.dashboard_recomendacoes_inovacao criada com sucesso")


# ─────────────────────────────────────────────
# Orquestração de Dashboards Metabase
# ─────────────────────────────────────────────

def setup_metabase_dashboards():
    """
    Cria automaticamente os dashboards no Metabase
    baseado nas tabelas gold criadas.
    """
    
    logger.info("\n" + "=" * 70)
    logger.info("Iniciando criação de Dashboards no Metabase")
    logger.info("=" * 70)
    
    try:
        # Autentica com Metabase
        api = MetabaseAPI(METABASE_HOST, METABASE_USER, METABASE_PASSWORD)
        
        if not api.authenticate():
            logger.warning("⚠ Metabase não disponível - pulando criação de dashboards")
            return False
        
        # Obtém IDs necessários
        db_id = api.get_db_id(POSTGRES_DB)
        if not db_id:
            logger.error("✗ Banco de dados não encontrado no Metabase")
            return False
        
        logger.info(f"✓ Banco de dados encontrado (ID: {db_id})")
        
        # Define configuração dos dashboards
        dashboards_config = {
            "dashboard_combinacoes_ouro": {
                "title": "🏆 Combinações de Ouro",
                "description": "Top pares de ingredientes que impulsionam produtos premium",
                "display_type": "table"
            },
            "dashboard_saturation_roi": {
                "title": "📊 Análise de Saturação & ROI",
                "description": "Curva de custo-benefício: quantidade ideal de ingredientes",
                "display_type": "scatter"
            },
            "dashboard_controverso_por_pele": {
                "title": "⚠️ Ingredientes Polêmicos vs Tipo de Pele",
                "description": "Impacto de alergênicos em diferentes tipos de pele",
                "display_type": "table"
            },
            "dashboard_premium_whitespace": {
                "title": "🚀 Oportunidades de Mercado (Whitespace)",
                "description": "Nichos inexplorados em Mid-ticket com combos premium",
                "display_type": "table"
            },
            "dashboard_benchmark_competitivo": {
                "title": "📈 Benchmark Competitivo",
                "description": "Posicionamento de marcas: qualidade vs preço",
                "display_type": "table"
            },
            "dashboard_recomendacoes_inovacao": {
                "title": "💡 Recomendações Executivas",
                "description": "Síntese de ações recomendadas para inovação",
                "display_type": "table"
            }
        }
        
        # Cria dashboard principal
        main_dash = api.create_dashboard(
            "🎯 Glow & Co - Business Intelligence",
            "Dashboard centralizado com análises estratégicas para decisões de negócio"
        )
        
        if not main_dash:
            logger.error("✗ Falha ao criar dashboard principal")
            return False
        
        main_dash_id = main_dash["id"]
        logger.info(f"✓ Dashboard principal criado (ID: {main_dash_id})")
        
        # Cria cards e adiciona ao dashboard
        current_row = 0
        for table_name, config in dashboards_config.items():
            logger.info(f"\n→ Processando: {config['title']}")
            
            # Obtém ID da tabela
            table_id = api.get_table_id(db_id, METABASE_SCHEMA, table_name)
            if not table_id:
                logger.warning(f"  ⚠ Tabela não encontrada: {table_name}")
                continue
            
            logger.info(f"  ✓ Tabela encontrada (ID: {table_id})")
            
            # Cria card
            card = api.create_card(table_id, config)
            if not card:
                logger.warning(f"  ⚠ Falha ao criar card para {table_name}")
                continue
            
            # Adiciona card ao dashboard
            if api.add_card_to_dashboard(main_dash_id, card["id"], current_row):
                current_row += 8
            else:
                logger.warning(f"  ⚠ Falha ao adicionar card ao dashboard")
        
        # Publica dashboard
        if api.publish_dashboard(main_dash_id):
            logger.info(f"\n✓ Dashboard publicado com sucesso!")
            logger.info(f"  Acesse em: {METABASE_HOST}/dashboard/{main_dash_id}")
        
        return True
        
    except Exception as e:
        logger.error(f"✗ Erro ao setup Metabase: {e}", exc_info=True)
        return False


# ─────────────────────────────────────────────
# ORQUESTRAÇÃO PRINCIPAL
# ─────────────────────────────────────────────

def main():
    """Executa o pipeline Gold completo."""
    
    try:
        logger.info("=" * 70)
        logger.info("Iniciando Pipeline Gold - Camada de Negócio Executiva")
        logger.info("=" * 70)
        
        engine = get_engine()
        
        # Verifica conexão
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
            conn.commit()
        logger.info("✓ Conexão com banco de dados estabelecida")
        
        # Cria schema gold
        create_gold_schema(engine)
        
        # ─ DASHBOARDS
        logger.info("\n[1/6] Criando Dashboard: Combinações de Ouro...")
        drop_table_if_exists(engine, "dashboard_combinacoes_ouro")
        create_dashboard_combinacoes_ouro(engine)
        
        logger.info("[2/6] Criando Dashboard: Análise de Saturação & ROI...")
        drop_table_if_exists(engine, "dashboard_saturation_roi")
        create_dashboard_saturation_roi(engine)
        
        logger.info("[3/6] Criando Dashboard: Impacto de Alergênicos por Pele...")
        drop_table_if_exists(engine, "dashboard_controverso_por_pele")
        create_dashboard_controverso_por_pele(engine)
        
        logger.info("[4/6] Criando Dashboard: Premium Whitespace (Oportunidades)...")
        drop_table_if_exists(engine, "dashboard_premium_whitespace")
        create_dashboard_premium_whitespace(engine)
        
        logger.info("[5/6] Criando Dashboard: Benchmark Competitivo...")
        drop_table_if_exists(engine, "dashboard_benchmark_competitivo")
        create_dashboard_benchmark_competitivo(engine)
        
        logger.info("[6/6] Criando Dashboard: Recomendações Executivas...")
        drop_table_if_exists(engine, "dashboard_recomendacoes_inovacao")
        create_dashboard_recomendacoes_inovacao(engine)
        
        # ─ Verifica dados criados
        logger.info("\n" + "─" * 70)
        logger.info("Verificando dados criados na camada Gold:")
        logger.info("─" * 70)
        
        dashboards = [
            "dashboard_combinacoes_ouro",
            "dashboard_saturation_roi",
            "dashboard_controverso_por_pele",
            "dashboard_premium_whitespace",
            "dashboard_benchmark_competitivo",
            "dashboard_recomendacoes_inovacao"
        ]
        
        with engine.connect() as conn:
            for table in dashboards:
                result = conn.execute(
                    text(f"SELECT COUNT(*) as cnt FROM gold.{table}")
                )
                count = result.scalar()
                logger.info(f"  ✓ gold.{table}: {count} linhas")
        
        logger.info("\n" + "=" * 70)
        logger.info("✓ Pipeline Gold (tabelas) concluído com sucesso!")
        logger.info("=" * 70)
        logger.info("\nDashboards criados na camada Gold:")
        for table in dashboards:
            logger.info(f"  • gold.{table}")
        
        # ─ SETUP METABASE
        logger.info("\n" + "=" * 70)
        setup_metabase_dashboards()
        logger.info("=" * 70)
        
        logger.info("\n✓ PIPELINE GOLD FINALIZADO COM SUCESSO!")
        
    except Exception as e:
        logger.error(f"✗ Erro no pipeline Gold: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
