"""
=============================================================
Glow & Co. — Pipeline Gold — Camada de Negócio Executiva
=============================================================
Responsabilidade:
  Criar tabelas de negócio (gold layer) prontas para
  visualização em dashboards (Metabase, Power BI, etc).
  
  Tabelas geradas:
  1. Dashboard_Combinacoes_Ouro: Recomendações de ingredientes
  2. Dashboard_Saturation_ROI: Análise custo-benefício
  3. Dashboard_Controverso_Por_Pele: Impacto de alergênicos
  4. Dashboard_Premium_Whitespace: Oportunidades de mercado
  5. Dashboard_Benchmark_Competitivo: Análise comparativa
  6. Dashboard_Recomendacoes_Inovacao: Sugestões de produtos

Execução:
  python gold/gold.py

Variáveis de ambiente (via .env):
  POSTGRES_HOST, POSTGRES_PORT, POSTGRES_USER,
  POSTGRES_PASSWORD, POSTGRES_DB
=============================================================
"""

from datetime import datetime, timezone
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import logging
import os
import sys


# ─────────────────────────────────────────────
# 1. Configuração de Logging
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("gold/gold.log", mode="a", encoding="utf-8"),
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
    Pronto para: Recomendações de formulação
    """
    
    sql = """
    CREATE TABLE gold.dashboard_combinacoes_ouro AS
    WITH top_pairs AS (
        SELECT
            ingredient_1 || ' + ' || ingredient_2 AS combinacao,
            ingredient_1,
            ingredient_2,
            'pair' AS tipo,
            total_products,
            avg_rank,
            pct_high_rank,
            avg_price,
            pct_premium,
            pct_mid_ticket
        FROM silver.ingredient_pairs_premium
        WHERE pct_high_rank >= 70
        LIMIT 20
    ),
    top_trios AS (
        SELECT
            ingredient_1 || ' + ' || ingredient_2 || ' + ' || ingredient_3 AS combinacao,
            NULL AS ingredient_1,
            NULL AS ingredient_2,
            'trio' AS tipo,
            total_products,
            avg_rank,
            pct_high_rank,
            avg_price,
            pct_premium,
            pct_mid_ticket
        FROM silver.ingredient_trios_premium
        WHERE pct_high_rank >= 70
        LIMIT 10
    )
    SELECT
        ROW_NUMBER() OVER (ORDER BY avg_rank DESC, pct_high_rank DESC) AS rank,
        combinacao,
        tipo,
        total_products AS "Produtos com essa combinação",
        ROUND(avg_rank, 2) AS "Rank Médio",
        ROUND(pct_high_rank, 1) AS "% Produtos com Rank > 4.5",
        ROUND(avg_price, 2) AS "Preço Médio (R$)",
        ROUND(pct_premium * 100, 1) AS "% no segmento Premium",
        ROUND(pct_mid_ticket * 100, 1) AS "% no segmento Mid-ticket",
        'Altamente recomendado para inovação' AS insight,
        NOW() AS data_atualizacao
    FROM (
        SELECT * FROM top_pairs
        UNION ALL
        SELECT * FROM top_trios
    ) combined
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
    Pronto para: Otimização de formulações
    """
    
    sql = """
    CREATE TABLE gold.dashboard_saturation_roi AS
    SELECT
        num_ingredients AS "Quantidade de Ingredientes",
        total_products AS "Total de Produtos",
        ROUND(avg_rank, 2) AS "Rank Médio",
        ROUND(avg_price, 2) AS "Preço Médio (R$)",
        ROUND(avg_price_per_ingredient, 2) AS "Custo por Ingrediente (R$)",
        ROUND(pct_high_rank, 1) AS "% Produtos com Rank Alto",
        ROUND(high_rank_count::numeric / total_products * 100, 1) AS "Taxa Sucesso (%)",
        trend AS "Tendência de Rank",
        ROUND(rank_improvement_per_price_delta, 4) AS "ROI (Melhoria Rank/R$)",
        CASE
            WHEN trend = 'improving' AND rank_improvement_per_price_delta > 0 THEN '✓ Investir'
            WHEN trend = 'stable' THEN '~ Manter'
            WHEN trend = 'declining' THEN '✗ Evitar'
            ELSE '?' 
        END AS recomendacao,
        CASE
            WHEN num_ingredients BETWEEN 8 AND 12 THEN 'Zona Ótima'
            WHEN num_ingredients < 8 THEN 'Subinvestido'
            WHEN num_ingredients > 15 THEN 'Saturado'
            ELSE 'Adequado'
        END AS zona_formulacao,
        NOW() AS data_atualizacao
    FROM silver.ingredient_saturation_analysis
    WHERE num_ingredients IS NOT NULL
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
    Pronto para: Análise de risco e oportunidade
    """
    
    sql = """
    CREATE TABLE gold.dashboard_controverso_por_pele AS
    SELECT
        UPPER(skin_type) AS "Tipo de Pele",
        ingredient_status AS "Status do Ingrediente",
        total_products AS "Total de Produtos",
        ROUND(avg_rank, 2) AS "Rank Médio",
        ROUND(avg_price, 2) AS "Preço Médio (R$)",
        ROUND(pct_high_rank, 1) AS "% com Rank Alto",
        ROUND(avg_num_ingredients, 1) AS "Média de Ingredientes",
        ROUND(rank_penalty, 2) AS "Penalidade de Rank",
        ROUND(pct_rank_penalty, 2) AS "Penalidade %",
        CASE
            WHEN skin_type = 'sensitive' AND rank_penalty < -0.5 THEN 'CRÍTICO: Afeta muito peles sensíveis'
            WHEN skin_type = 'sensitive' AND rank_penalty < -0.2 THEN 'Alto impacto em peles sensíveis'
            WHEN rank_penalty < -0.1 THEN 'Impacto moderado'
            WHEN rank_penalty IS NULL THEN 'Não aplicável'
            ELSE 'Impacto baixo'
        END AS alerta_risco,
        NOW() AS data_atualizacao
    FROM silver.controversial_ingredients_impact
    ORDER BY 
        CASE WHEN skin_type = 'sensitive' THEN 0 ELSE 1 END,
        rank_penalty ASC NULLS LAST
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
    Pronto para: Identificar nichos inexplorados
    """
    
    sql = """
    CREATE TABLE gold.dashboard_premium_whitespace AS
    WITH high_performers AS (
        SELECT
            ingredient_1,
            ingredient_2,
            avg_rank,
            avg_price,
            pct_premium,
            pct_mid_ticket,
            CASE
                WHEN pct_premium > 0.5 AND pct_mid_ticket < 0.1 THEN 'Premium Exclusivo'
                WHEN pct_premium > 0.3 AND pct_mid_ticket > 0.3 THEN 'Híbrido'
                ELSE 'Mixed'
            END AS segmento
        FROM silver.ingredient_pairs_premium
        WHERE pct_high_rank >= 70
    )
    SELECT
        ROW_NUMBER() OVER (
            PARTITION BY segmento 
            ORDER BY avg_rank DESC
        ) AS rank_no_segmento,
        ingredient_1 || ' + ' || ingredient_2 AS combinacao_recomendada,
        segmento,
        ROUND(avg_rank, 2) AS "Rank Esperado",
        ROUND(avg_price, 2) AS "Preço de Referência (Premium)",
        ROUND(avg_price * 0.5, 2) AS "Preço Sugerido (Mid-ticket)",
        CASE
            WHEN pct_mid_ticket < 0.1 THEN 'ALTA: Nicho inexplorado'
            WHEN pct_mid_ticket BETWEEN 0.1 AND 0.2 THEN 'MÉDIA: Pouca competição'
            ELSE 'BAIXA: Mercado competitivo'
        END AS potencial_whitespace,
        'Aplicar essa combinação em produto de R$ ' || 
        ROUND(avg_price * 0.5, 2) || ' pode capturar novo nicho' AS oportunidade,
        NOW() AS data_atualizacao
    FROM high_performers
    WHERE pct_mid_ticket < 0.3  -- Filtro: pouca penetração em mid-ticket
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
    Pronto para: Análise competitiva
    """
    
    sql = """
    CREATE TABLE gold.dashboard_benchmark_competitivo AS
    WITH produtos_base AS (
        SELECT
            brand,
            COUNT(*) AS total_produtos,
            ROUND(AVG(rank), 2) AS rank_medio_marca,
            ROUND(AVG(price), 2) AS preco_medio_marca,
            COUNT(CASE WHEN rank >= 4.5 THEN 1 END) AS produtos_premium_rank,
            ROUND(
                100.0 * COUNT(CASE WHEN rank >= 4.5 THEN 1 END) / COUNT(*),
                1
            ) AS pct_premium_rank,
            COUNT(CASE WHEN price > 80 THEN 1 END) AS produtos_preco_premium,
            ROUND(MIN(price), 2) AS preco_minimo,
            ROUND(MAX(price), 2) AS preco_maximo
        FROM public.cosmetics_products
        WHERE brand IS NOT NULL
        GROUP BY brand
        HAVING COUNT(*) >= 2  -- Apenas marcas com 2+ produtos
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
        ROUND(pb.rank_medio_marca, 2) AS "Rank Médio",
        CASE
            WHEN pb.rank_medio_marca >= ms.rank_q75 THEN '⭐⭐⭐ Premium'
            WHEN pb.rank_medio_marca >= ms.rank_mercado_medio THEN '⭐⭐ Acima da média'
            ELSE '⭐ Abaixo da média'
        END AS "Posição de Qualidade",
        ROUND(pb.preco_medio_marca, 2) AS "Preço Médio (R$)",
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
            'Usar combinações que têm 70%+ de sucesso em Premium, mas <20% em Mid-ticket' AS acao,
            'Capturar whitespace com produtos de R$ 40-60 que atingem Rank 4.5+' AS resultado_esperado,
            (SELECT COUNT(*) FROM silver.ingredient_pairs_premium WHERE pct_high_rank >= 70)::text AS casos_suporte
        
        UNION ALL
        
        -- REC 2: Ponto ótimo de ingredientes
        SELECT
            'Otimizar Quantidade de Ingredientes',
            2,
            'ALTA',
            'Implementar em 60 dias',
            'Limitar formulações a 10-12 ingredientes (zona ótima) para maximizar ROI',
            'Reduzir custo de produção enquanto mantém/melhora Rank',
            (SELECT COUNT(*) FROM silver.ingredient_saturation_analysis 
             WHERE trend = 'improving' AND num_ingredients BETWEEN 8 AND 12)::text
        
        UNION ALL
        
        -- REC 3: Cuidado com alergênicos em pele sensível
        SELECT
            'Evitar Ingredientes Polêmicos em Pele Sensível',
            1,
            'CRÍTICA',
            'Imediato',
            'Reformular produtos para pele sensível sem ingredientes polêmicos (parabens, sulfatos, etc)',
            'Aumentar Rank em 0.3-0.5 pontos, melhorando retenção de clientes',
            (SELECT COUNT(CASE WHEN skin_type = 'sensitive' AND rank_penalty < -0.3 THEN 1 END) 
             FROM silver.controversial_ingredients_impact)::text
        
        UNION ALL
        
        -- REC 4: Benchmark contra concorrência
        SELECT
            'Analisar Posicionamento vs Concorrência',
            3,
            'MÉDIA',
            'Análise em 30 dias',
            'Revisar posicionamento de preço e qualidade vs líderes de mercado',
            'Definir estratégia clara: Premium, Value, ou Nicho',
            (SELECT COUNT(*) FROM gold.dashboard_benchmark_competitivo 
             WHERE "Posição de Qualidade" = '⭐⭐⭐ Premium')::text
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
# ORQUESTRAÇÃO PRINCIPAL
# ─────────────────────────────────────────────

def main():
    """Executa o pipeline Gold."""
    
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
        logger.info("✓ Pipeline Gold concluído com sucesso!")
        logger.info("=" * 70)
        logger.info("\nDashboards disponíveis para Metabase:")
        for table in dashboards:
            logger.info(f"  • gold.{table}")
        
    except Exception as e:
        logger.error(f"Erro no pipeline Gold: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
