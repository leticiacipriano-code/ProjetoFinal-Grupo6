# 📊 Metabase Setup Completo

## Visão Geral

O arquivo `metabase_setup.py` é o **único arquivo necessário** para provisionar o Metabase com:
- ✅ Dashboards automáticos
- ✅ Cards (visualizações) com gráficos
- ✅ Tipos de visualização: bar, scatter, row, table, line, etc.

Consolida a funcionalidade de:
- `metabase_provisioning.py` (provisioning)
- `visualization_types_reference.py` (tipos de gráficos)

## 🚀 Uso

### Execução Automática
```bash
python main.py
```
O metabase_setup.py é chamado automaticamente na **Etapa 6** do pipeline.

### Execução Manual
```bash
# Setup completo
docker-compose exec g6 python provisioning/metabase_setup.py

# Ver referência de tipos de visualização
docker-compose exec g6 python provisioning/metabase_setup.py --ref
```

## 📋 Estrutura

### Seção 1: DASHBOARDS_CONFIG
Define cada dashboard com:
- `title` - Nome do dashboard
- `description` - Descrição
- `display_type` - Tipo de gráfico (bar, scatter, row, table, etc)
- `visualization` - Configurações específicas do tipo

Exemplo:
```python
DASHBOARDS_CONFIG = {
    "dashboard_combinacoes_ouro": {
        "title": "🏆 Combinações de Ouro",
        "description": "Top combinações de ingredientes...",
        "display_type": "bar",  # ← Tipo de gráfico
        "visualization": {
            "bar": {
                "title": "Pares de Ingredientes Premium",
                "show_legend": True,
            }
        }
    },
    # ... mais dashboards
}
```

### Seção 2: VISUALIZATION_TYPES_REFERENCE
Referência completa de todos os tipos disponíveis:
- `table` - Tabela
- `bar` - Barras verticais
- `row` - Barras horizontais
- `scatter` - Dispersão
- `line` - Linhas
- `number` - Número único
- `gauge` - Medidor
- `pivot` - Tabela dinâmica

### Seção 3: Classe MetabaseAPI
Cliente Python para interagir com API REST do Metabase.

### Seção 4: Funções Principais
- `setup_metabase()` - Executa setup completo
- `print_visualization_reference()` - Mostra referência

## 🎨 Customizar Gráficos

### Mudar tipo de gráfico

Edite `DASHBOARDS_CONFIG` no arquivo:

```python
"dashboard_combinacoes_ouro": {
    "display_type": "bar",  # Mude para: scatter, row, line, etc
}
```

### Mudar configurações do gráfico

```python
"visualization": {
    "bar": {
        "title": "Novo título",
        "show_legend": False,  # Esconder legenda
        "stacked": True,       # Barras empilhadas
    }
}
```

## 📊 Dashboards Criados

| Nome | Gráfico | Descrição |
|------|---------|-----------|
| 🏆 Combinações de Ouro | BAR | Top pares de ingredientes |
| 📊 Saturação & ROI | SCATTER | Relação ingredientes vs ROI |
| ⚠️ Polêmicos por Pele | ROW | Impacto por tipo de pele |
| 🚀 Premium Whitespace | SCATTER | Oportunidades de mercado |
| 🎯 Benchmark Competitivo | BAR | Análise competitiva |
| 💡 Recomendações | TABLE | Sugestões de ação |

## 🔧 Troubleshooting

### Metabase não respondeu
```bash
# Verifique se Metabase está rodando
docker-compose ps metabase

# Veja os logs
docker-compose logs -f metabase
```

### Tabelas não encontradas
```bash
# Certifique-se que Gold foi executado
docker-compose exec g6 python gold/gold.py
```

### Visualizações criadas mas sem dados
```bash
# Execute o pipeline completo
docker-compose exec g6 python main.py
```

## 📝 Logs

Os logs são salvos em:
```
provisioning/metabase_setup.log
```

Verifique para debugging:
```bash
tail -f provisioning/metabase_setup.log
```

## 🎯 Arquivos Substituídos

Este arquivo `metabase_setup.py` **substitui**:
- ~~metabase_provisioning.py~~ ❌ (legado)
- ~~visualization_types_reference.py~~ ❌ (integrado)

## 📚 Referência Rápida

### Tipos de Visualização

```bash
# Ver todos os tipos
python provisioning/metabase_setup.py --ref
```

### Estrutura do DASHBOARDS_CONFIG

```python
{
    "nome_tabela": {
        "title": "Título do Dashboard",
        "description": "Descrição",
        "display_type": "tipo",  # bar, scatter, row, table, line, number, gauge, pivot
        "visualization": {
            "tipo": {
                "custom_setting_1": "value",
                "custom_setting_2": "value",
            }
        }
    }
}
```

---

**Última atualização**: April 20, 2026
