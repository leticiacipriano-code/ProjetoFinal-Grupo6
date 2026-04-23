"""
=============================================================
Metabase Setup Pro - Gráficos e Dashboards Automáticos
=============================================================
"""

import requests
import json
import logging
import sys
import time
from typing import Dict, List, Optional

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# --- CONFIGURAÇÕES ---
METABASE_HOST = "http://metabase:3000" # Use 'metabase' se rodar dentro do container ou 'localhost' fora
METABASE_USER = "admin@metabase.local"
METABASE_PASSWORD = "metabase"
DB_NAME = "glow_db"
SCHEMA = "gold"

# IMPORTANTE: Substitua 'x' e 'y' pelos nomes reais das suas colunas na camada GOLD
DASHBOARDS_CONFIG = {
    "dashboard_combinacoes_ouro": {
        "title": "🏆 Combinações de Ouro",
        "description": "Top combinações de ingredientes",
        "display_type": "bar",
        "x": "ingredientes_combinados", # <--- COLOQUE O NOME DA COLUNA AQUI
        "y": "total_vendas",            # <--- COLOQUE O NOME DA COLUNA AQUI
    },
    "dashboard_saturation_roi": {
        "title": "📊 Análise de Saturação & ROI",
        "description": "Saturação vs custo-benefício",
        "display_type": "scatter",
        "x": "n_ingredientes",
        "y": "roi_percentual",
    },
    "dashboard_premium_whitespace": {
        "title": "🚀 Oportunidades de Mercado",
        "description": "Preço vs Rank",
        "display_type": "scatter",
        "x": "price_usd",
        "y": "avg_rank",
    }
}

class MetabaseAPI:
    def __init__(self, host, username, password):
        self.host = host
        self.base_url = f"{host}/api"
        self.auth = {"username": username, "password": password}
        self.session_id = None

    def authenticate(self):
        try:
            resp = requests.post(f"{self.base_url}/session", json=self.auth, timeout=10)
            if resp.status_code == 200:
                self.session_id = resp.json()["id"]
                logger.info("✓ Autenticado")
                return True
        except Exception as e:
            logger.error(f"Erro auth: {e}")
        return False

    def get_headers(self):
        return {"X-Metabase-Session": self.session_id, "Content-Type": "application/json"}

    def get_db_id(self, name):
        resp = requests.get(f"{self.base_url}/database", headers=self.get_headers())
        for db in resp.json():
            if db["name"] == name: return db["id"]
        return None

    def get_table_id(self, db_id, schema, table_name):
        resp = requests.get(f"{self.base_url}/database/{db_id}/metadata", headers=self.get_headers())
        for t in resp.json().get("tables", []):
            if t["schema"] == schema and t["name"] == table_name: return t["id"]
        return None

    def create_card(self, table_id, config):
        # Monta as configurações de visualização para forçar o gráfico
        viz_settings = {
            "graph.dimensions": [config["x"]] if "x" in config else [],
            "graph.metrics": [config["y"]] if "y" in config else [],
            "graph.show_values": True,
        }

        payload = {
            "name": config["title"],
            "description": config.get("description", ""),
            "display": config["display_type"],
            "visualization_settings": viz_settings,
            "dataset_query": {
                "database": None,
                "type": "query",
                "query": {"source-table": table_id}
            }
        }

        resp = requests.post(f"{self.base_url}/card", headers=self.get_headers(), json=payload)
        if resp.status_code == 200:
            logger.info(f"✓ Card criado: {config['title']}")
            return resp.json()
        logger.error(f"Falha card {config['title']}: {resp.text}")
        return None

    def create_dashboard(self, name):
        payload = {"name": name}
        resp = requests.post(f"{self.base_url}/dashboard", headers=self.get_headers(), json=payload)
        return resp.json() if resp.status_code == 200 else None

    def add_card_to_dash(self, dash_id, card_id, row):
        payload = {
            "card_id": card_id,
            "row": row, "col": 0, "size_x": 12, "size_y": 8
        }
        requests.post(f"{self.base_url}/dashboard/{dash_id}/cards", headers=self.get_headers(), json=payload)

def setup_metabase():
    api = MetabaseAPI(METABASE_HOST, METABASE_USER, METABASE_PASSWORD)
    if not api.authenticate(): return

    db_id = api.get_db_id(DB_NAME)
    if not db_id: 
        logger.error("Banco não encontrado!")
        return

    dash = api.create_dashboard("🎯 Glow & Co - Business Intelligence")
    if not dash: return
    
    current_row = 0
    for table_name, config in DASHBOARDS_CONFIG.items():
        t_id = api.get_table_id(db_id, SCHEMA, table_name)
        if t_id:
            card = api.create_card(t_id, config)
            if card:
                api.add_card_to_dash(dash["id"], card["id"], current_row)
                current_row += 8
    
    logger.info(f"\n🚀 TUDO PRONTO! Acesse o dashboard no Metabase.")

if __name__ == "__main__":
    setup_metabase()