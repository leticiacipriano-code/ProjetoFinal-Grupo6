"""
=============================================================
Glow & Co. — Pipeline Silver — Camada de Análise Avançada
=============================================================
Responsabilidade:
  Criar tabelas analíticas (silver layer) que respondem às
  perguntas de negócio estratégicas sobre:
  
  1. Combinações de ouro (pares/trios de ingredientes)
     que impulsionam o Rank de produtos Premium
  
  2. Ponto de saturação: quantos ingredientes é ótimo antes
     de apenas encarecer o preço sem melhorar o Rank
  
  3. Impacto de ingredientes polêmicos/alergênicos em
     produtos para peles sensíveis vs outros tipos

Execução:
  python silver/silver.py

Variáveis de ambiente (via .env):
  POSTGRES_HOST, POSTGRES_PORT, POSTGRES_USER,
  POSTGRES_PASSWORD, POSTGRES_DB
=============================================================
"""

from datetime import datetime, timezone
from dotenv import load_dotenv
from sqlalchemy import create_engine, text, MetaData, Table, Column, Integer, String, Float, DateTime, Boolean, inspect
import logging
import os
import sys
import pandas as pd


# ─────────────────────────────────────────────
# 1. Configuração de Logging
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("silver/silver.log", mode="a", encoding="utf-8"),
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
# 3. Funções para criar tabelas Silver
# ─────────────────────────────────────────────

def get_engine():
    """Cria conexão com o banco de dados PostgreSQL."""
    return create_engine(DB_URL, echo=False)


def create_silver_schema(engine):
    """Cria o schema silver se não existir."""
    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS silver"))
        conn.commit()
    logger.info("Schema 'silver' verificado/criado com sucesso")


def drop_table_if_exists(engine, table_name):
    """Dropa a tabela se existir."""
    with engine.connect() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS silver.{table_name} CASCADE"))
        conn.commit()
    logger.info(f"Tabela silver.{table_name} dropada (se existia)")


# ─────────────────────────────────────────────
# PERGUNTA 1: Combinações de Ouro (Pares/Trios)
# ─────────────────────────────────────────────

def create_ingredient_pairs_premium(engine):
    """
    Pergunta 1: Identifica pares e trios de ingredientes
    que impulsionam o Rank (nota > 4.5) de produtos Premium.
    
    Nicho: produtos de ticket médio que poderiam usar essas
    combinações para melhorar suas classificações.
    """
    
    sql = """
    -- Tabela: silver.ingredient_pairs_premium
    -- Pergunta: Quais combinações de ingredientes impulsionam o Rank premium?
    
    CREATE TABLE silver.ingredient_pairs_premium AS
    WITH cosmetics_preprocessed AS (
        SELECT
            name AS product_name,
            brand,
            price,
            rank,
            CASE
                WHEN price > 80 THEN 'Premium'
                WHEN price BETWEEN 30 AND 80 THEN 'Mid-ticket'
                ELSE 'Economy'
            END AS price_segment,
            string_to_array(
                TRIM(ingredients_list, '[]'), ', '
            ) AS ingredients_array,
            array_length(
                string_to_array(TRIM(ingredients_list, '[]'), ', '), 1
            ) AS num_ingredients,
            CASE
                WHEN rank >= 4.5 THEN 'high_rank'
                WHEN rank >= 3.5 THEN 'medium_rank'
                ELSE 'low_rank'
            END AS rank_category
        FROM staging.stg_cosmetics_products
        WHERE ingredients_list IS NOT NULL
    ),
    
    -- Extrai pares de ingredientes
    ingredient_pairs AS (
        SELECT
            cp.product_name,
            cp.brand,
            cp.price,
            cp.rank,
            cp.price_segment,
            cp.rank_category,
            cp.num_ingredients,
            ing1.ing AS ingredient_1,
            ing2.ing AS ingredient_2,
            ing1.idx < ing2.idx AS is_pair
        FROM cosmetics_preprocessed cp
        CROSS JOIN LATERAL UNNEST(cp.ingredients_array) WITH ORDINALITY AS ing1(ing, idx)
        CROSS JOIN LATERAL UNNEST(cp.ingredients_array) WITH ORDINALITY AS ing2(ing, idx)
        WHERE ing1.idx < ing2.idx
    ),
    
    -- Agrega estatísticas por par de ingredientes
    pair_statistics AS (
        SELECT
            ingredient_1,
            ingredient_2,
            COUNT(*) AS total_products,
            ROUND(AVG(rank)::numeric, 2) AS avg_rank,
            COUNT(CASE WHEN rank_category = 'high_rank' THEN 1 END) AS high_rank_count,
            ROUND(
                100.0 * COUNT(CASE WHEN rank_category = 'high_rank' THEN 1 END) / COUNT(*)::numeric,
                2
            ) AS pct_high_rank,
            ROUND(AVG(CASE WHEN price_segment = 'Premium' THEN 1 ELSE 0 END)::numeric, 2) AS pct_premium,
            ROUND(AVG(CASE WHEN price_segment = 'Mid-ticket' THEN 1 ELSE 0 END)::numeric, 2) AS pct_mid_ticket,
            ROUND(AVG(price)::numeric, 2) AS avg_price,
            'pair' AS combination_type,
            NOW() AS created_at
        FROM ingredient_pairs
        GROUP BY ingredient_1, ingredient_2
        HAVING COUNT(*) >= 2  -- Apenas combinações que aparecem em pelo menos 2 produtos
        ORDER BY avg_rank DESC, total_products DESC
    )
    
    SELECT *
    FROM pair_statistics
    WHERE pct_high_rank >= 50  -- Filtro: pelo menos 50% dos produtos com essa combinação têm rank alto
    """
    
    with engine.connect() as conn:
        conn.execute(text(sql))
        conn.commit()
    
    logger.info("Tabela silver.ingredient_pairs_premium criada com sucesso")


def create_ingredient_trios_premium(engine):
    """
    Extensão da Pergunta 1: Identifica trios de ingredientes
    que impulsionam o Rank (nota > 4.5) de produtos Premium.
    """
    
    sql = """
    CREATE TABLE silver.ingredient_trios_premium AS
    WITH cosmetics_preprocessed AS (
        SELECT
            name AS product_name,
            brand,
            price,
            rank,
            CASE
                WHEN price > 80 THEN 'Premium'
                WHEN price BETWEEN 30 AND 80 THEN 'Mid-ticket'
                ELSE 'Economy'
            END AS price_segment,
            string_to_array(
                TRIM(ingredients_list, '[]'), ', '
            ) AS ingredients_array,
            array_length(
                string_to_array(TRIM(ingredients_list, '[]'), ', '), 1
            ) AS num_ingredients,
            CASE
                WHEN rank >= 4.5 THEN 'high_rank'
                WHEN rank >= 3.5 THEN 'medium_rank'
                ELSE 'low_rank'
            END AS rank_category
        FROM staging.stg_cosmetics_products
        WHERE ingredients_list IS NOT NULL
    ),
    
    -- Extrai trios de ingredientes
    ingredient_trios AS (
        SELECT
            cp.product_name,
            cp.brand,
            cp.price,
            cp.rank,
            cp.price_segment,
            cp.rank_category,
            cp.num_ingredients,
            ing1.ing AS ingredient_1,
            ing2.ing AS ingredient_2,
            ing3.ing AS ingredient_3
        FROM cosmetics_preprocessed cp
        CROSS JOIN LATERAL UNNEST(cp.ingredients_array) WITH ORDINALITY AS ing1(ing, idx)
        CROSS JOIN LATERAL UNNEST(cp.ingredients_array) WITH ORDINALITY AS ing2(ing, idx)
        CROSS JOIN LATERAL UNNEST(cp.ingredients_array) WITH ORDINALITY AS ing3(ing, idx)
        WHERE ing1.idx < ing2.idx AND ing2.idx < ing3.idx
    ),
    
    -- Agrega estatísticas por trio de ingredientes
    trio_statistics AS (
        SELECT
            ingredient_1,
            ingredient_2,
            ingredient_3,
            COUNT(*) AS total_products,
            ROUND(AVG(rank)::numeric, 2) AS avg_rank,
            COUNT(CASE WHEN rank_category = 'high_rank' THEN 1 END) AS high_rank_count,
            ROUND(
                100.0 * COUNT(CASE WHEN rank_category = 'high_rank' THEN 1 END) / COUNT(*)::numeric,
                2
            ) AS pct_high_rank,
            ROUND(AVG(CASE WHEN price_segment = 'Premium' THEN 1 ELSE 0 END)::numeric, 2) AS pct_premium,
            ROUND(AVG(CASE WHEN price_segment = 'Mid-ticket' THEN 1 ELSE 0 END)::numeric, 2) AS pct_mid_ticket,
            ROUND(AVG(price)::numeric, 2) AS avg_price,
            'trio' AS combination_type,
            NOW() AS created_at
        FROM ingredient_trios
        GROUP BY ingredient_1, ingredient_2, ingredient_3
        HAVING COUNT(*) >= 2
        ORDER BY avg_rank DESC, total_products DESC
    )
    
    SELECT *
    FROM trio_statistics
    WHERE pct_high_rank >= 50
    """
    
    with engine.connect() as conn:
        conn.execute(text(sql))
        conn.commit()
    
    logger.info("Tabela silver.ingredient_trios_premium criada com sucesso")


# ─────────────────────────────────────────────
# PERGUNTA 2: Ponto de Saturação de Ingredientes
# ─────────────────────────────────────────────

def create_saturation_analysis(engine):
    """
    Pergunta 2: Identifica o ponto de saturação onde
    adicionar mais ingredientes não melhora o Rank,
    apenas aumenta o preço (custo-benefício).
    """
    
    sql = """
    CREATE TABLE silver.ingredient_saturation_analysis AS
    WITH cosmetics_analyzed AS (
        SELECT
            name AS product_name,
            brand,
            price,
            rank,
            CASE
                WHEN price > 80 THEN 'Premium'
                WHEN price BETWEEN 30 AND 80 THEN 'Mid-ticket'
                ELSE 'Economy'
            END AS price_segment,
            array_length(
                string_to_array(TRIM(ingredients_list, '[]'), ', '), 1
            ) AS num_ingredients,
            ROUND(price / NULLIF(
                array_length(string_to_array(TRIM(ingredients_list, '[]'), ', '), 1), 0
            ), 2) AS price_per_ingredient,
            CASE
                WHEN rank >= 4.5 THEN 'high_rank'
                WHEN rank >= 3.5 THEN 'medium_rank'
                ELSE 'low_rank'
            END AS rank_category
        FROM staging.stg_cosmetics_products
        WHERE ingredients_list IS NOT NULL
            AND array_length(string_to_array(TRIM(ingredients_list, '[]'), ', '), 1) > 0
    ),
    
    -- Agrupa por quantidade de ingredientes
    saturation_by_count AS (
        SELECT
            num_ingredients,
            COUNT(*) AS total_products,
            ROUND(AVG(rank)::numeric, 2) AS avg_rank,
            ROUND(AVG(price)::numeric, 2) AS avg_price,
            ROUND(AVG(price_per_ingredient)::numeric, 2) AS avg_price_per_ingredient,
            COUNT(CASE WHEN rank_category = 'high_rank' THEN 1 END) AS high_rank_count,
            ROUND(
                100.0 * COUNT(CASE WHEN rank_category = 'high_rank' THEN 1 END) / COUNT(*)::numeric,
                2
            ) AS pct_high_rank,
            ROUND(MIN(rank)::numeric, 2) AS min_rank,
            ROUND(MAX(rank)::numeric, 2) AS max_rank,
            ROUND(STDDEV(rank)::numeric, 2) AS stddev_rank,
            NOW() AS created_at
        FROM cosmetics_analyzed
        GROUP BY num_ingredients
        ORDER BY num_ingredients
    )
    
    -- Calcula a curva de custo-benefício
    SELECT
        num_ingredients,
        total_products,
        avg_rank,
        avg_price,
        avg_price_per_ingredient,
        high_rank_count,
        pct_high_rank,
        min_rank,
        max_rank,
        stddev_rank,
        -- Identifica o ponto de saturação (quando o rank passa a cair)
        CASE
            WHEN LAG(avg_rank) OVER (ORDER BY num_ingredients) IS NULL THEN 'baseline'
            WHEN avg_rank < LAG(avg_rank) OVER (ORDER BY num_ingredients) THEN 'declining'
            WHEN avg_rank > LAG(avg_rank) OVER (ORDER BY num_ingredients) THEN 'improving'
            ELSE 'stable'
        END AS trend,
        -- ROI: quanto melhora o rank por real investido em novos ingredientes
        ROUND(
            (avg_rank - LAG(avg_rank, 1, avg_rank) OVER (ORDER BY num_ingredients)) /
            NULLIF((avg_price - LAG(avg_price, 1, avg_price) OVER (ORDER BY num_ingredients)), 0),
            4
        ) AS rank_improvement_per_price_delta,
        created_at
    FROM saturation_by_count
    """
    
    with engine.connect() as conn:
        conn.execute(text(sql))
        conn.commit()
    
    logger.info("Tabela silver.ingredient_saturation_analysis criada com sucesso")


# ─────────────────────────────────────────────
# PERGUNTA 3: Ingredientes Polêmicos & Pele Sensível
# ─────────────────────────────────────────────

def create_controversial_ingredients_impact(engine):
    """
    Pergunta 3: Analisa se ingredientes polêmicos/alergênicos
    penalizam mais o Rank de produtos para peles sensíveis
    comparado a outros tipos de pele.
    """
    
    sql = """
    CREATE TABLE silver.controversial_ingredients_impact AS
    WITH controversial_list AS (
        -- Lista de ingredientes conhecidos como polêmicos/alergênicos
        SELECT 'sodium lauryl sulfate' AS ingredient
        UNION ALL SELECT 'parabens'
        UNION ALL SELECT 'sulfate'
        UNION ALL SELECT 'alcohol denat'
        UNION ALL SELECT 'fragrance'
        UNION ALL SELECT 'parfum'
        UNION ALL SELECT 'phenoxyethanol'
        UNION ALL SELECT 'formaldehyde'
        UNION ALL SELECT 'DEA'
        UNION ALL SELECT 'dmdm hydantoin'
    ),
    
    cosmetics_with_flags AS (
        SELECT
            name AS product_name,
            brand,
            price,
            rank,
            ingredients_list,
            CASE
                WHEN sensitive = 1 OR sensitive = true THEN 'sensitive'
                WHEN oily = 1 OR oily = true THEN 'oily'
                WHEN dry = 1 OR dry = true THEN 'dry'
                ELSE 'combination'
            END AS skin_type,
            -- Verifica presença de ingredientes polêmicos
            CASE
                WHEN (
                    LOWER(ingredients_list) ~ 'sodium lauryl sulfate'
                    OR LOWER(ingredients_list) ~ 'parabens'
                    OR LOWER(ingredients_list) ~ 'sulfate'
                    OR LOWER(ingredients_list) ~ 'alcohol denat'
                    OR LOWER(ingredients_list) ~ 'fragrance'
                    OR LOWER(ingredients_list) ~ 'parfum'
                    OR LOWER(ingredients_list) ~ 'phenoxyethanol'
                    OR LOWER(ingredients_list) ~ 'formaldehyde'
                ) THEN 1 ELSE 0
            END AS has_controversial_ingredient,
            array_length(
                string_to_array(TRIM(ingredients_list, '[]'), ', '), 1
            ) AS num_ingredients
        FROM staging.stg_cosmetics_products
        WHERE ingredients_list IS NOT NULL
    ),
    
    -- Agrupa por tipo de pele e presença de ingredientes polêmicos
    impact_analysis AS (
        SELECT
            skin_type,
            has_controversial_ingredient,
            CASE WHEN has_controversial_ingredient = 1 THEN 'with_controversial' ELSE 'without_controversial' END AS ingredient_status,
            COUNT(*) AS total_products,
            ROUND(AVG(rank)::numeric, 2) AS avg_rank,
            ROUND(AVG(price)::numeric, 2) AS avg_price,
            COUNT(CASE WHEN rank >= 4.5 THEN 1 END) AS high_rank_count,
            ROUND(
                100.0 * COUNT(CASE WHEN rank >= 4.5 THEN 1 END) / COUNT(*)::numeric,
                2
            ) AS pct_high_rank,
            ROUND(AVG(num_ingredients)::numeric, 2) AS avg_num_ingredients,
            NOW() AS created_at
        FROM cosmetics_with_flags
        GROUP BY skin_type, has_controversial_ingredient
    )
    
    SELECT
        skin_type,
        ingredient_status,
        total_products,
        avg_rank,
        avg_price,
        high_rank_count,
        pct_high_rank,
        avg_num_ingredients,
        -- Calcula o impacto (diferença de rank)
        ROUND(
            avg_rank - MAX(CASE WHEN ingredient_status = 'without_controversial' THEN avg_rank END)
            OVER (PARTITION BY skin_type),
            2
        ) AS rank_penalty,
        -- Percentual de diferença
        ROUND(
            100.0 * (avg_rank - MAX(CASE WHEN ingredient_status = 'without_controversial' THEN avg_rank END)
            OVER (PARTITION BY skin_type)) /
            NULLIF(MAX(CASE WHEN ingredient_status = 'without_controversial' THEN avg_rank END)
            OVER (PARTITION BY skin_type), 0),
            2
        ) AS pct_rank_penalty,
        created_at
    FROM impact_analysis
    ORDER BY skin_type, ingredient_status
    """
    
    with engine.connect() as conn:
        conn.execute(text(sql))
        conn.commit()
    
    logger.info("Tabela silver.controversial_ingredients_impact criada com sucesso")


# ─────────────────────────────────────────────
# ORQUESTRAÇÃO PRINCIPAL
# ─────────────────────────────────────────────

def main():
    """Executa o pipeline Silver."""
    
    try:
        logger.info("=" * 60)
        logger.info("Iniciando Pipeline Silver - Análise Avançada")
        logger.info("=" * 60)
        
        engine = get_engine()
        
        # Verifica conexão
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
            conn.commit()
        logger.info("✓ Conexão com banco de dados estabelecida")
        
        # Cria schema silver
        create_silver_schema(engine)
        
        # ─ PERGUNTA 1: Combinações de Ouro
        logger.info("\n[1/5] Criando tabela de pares de ingredientes premium...")
        drop_table_if_exists(engine, "ingredient_pairs_premium")
        create_ingredient_pairs_premium(engine)
        
        logger.info("[2/5] Criando tabela de trios de ingredientes premium...")
        drop_table_if_exists(engine, "ingredient_trios_premium")
        create_ingredient_trios_premium(engine)
        
        # ─ PERGUNTA 2: Ponto de Saturação
        logger.info("[3/5] Criando análise de saturação de ingredientes...")
        drop_table_if_exists(engine, "ingredient_saturation_analysis")
        create_saturation_analysis(engine)
        
        # ─ PERGUNTA 3: Ingredientes Polêmicos
        logger.info("[4/5] Criando análise de ingredientes polêmicos...")
        drop_table_if_exists(engine, "controversial_ingredients_impact")
        create_controversial_ingredients_impact(engine)
        
        # ─ Verifica dados criados
        logger.info("[5/5] Verificando dados criados...")
        with engine.connect() as conn:
            for table in [
                "ingredient_pairs_premium",
                "ingredient_trios_premium",
                "ingredient_saturation_analysis",
                "controversial_ingredients_impact"
            ]:
                result = conn.execute(
                    text(f"SELECT COUNT(*) as cnt FROM silver.{table}")
                )
                count = result.scalar()
                logger.info(f"  ✓ silver.{table}: {count} linhas")
        
        logger.info("\n" + "=" * 60)
        logger.info("Pipeline Silver concluído com sucesso!")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"Erro no pipeline Silver: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
