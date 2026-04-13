import pandas as pd


df_dict  = [
        {
            "ingrediente": "paraben","categoria": "Conservante", "risco": "Alto", "motivo": "Desregulação Endócrina",
        },
        {
            "ingrediente": "sulfate","categoria": "Surfactante","risco": "Médio","motivo": "Irritação Cutânea/Ressecamento",
        },
        {
            "ingrediente": "fragrance", "categoria": "Fragrância", "risco": "Alto", "motivo": "Alergia não especificada",
        },
        {
            "ingrediente": "phthalate", "categoria": "Fixador", "risco": "Alto", "motivo": "Toxicidade Reprodutiva",
        },
        {
            "ingrediente": "formaldehyde", "categoria": "Conservante", "risco": "Muito Alto", "motivo": "Carcinogénico/Alergia Severa",
        },
        {
            "ingrediente": "linalool","categoria": "Fragrância Natural", "risco": "Baixo", "motivo": "Sensibilizante em oxidação",
        },
    ]

df_ingredientes = pd.read_csv(r"C:\Users\letic\OneDrive\Documentos\ProjetoFinal-Grupo6\base\COSING_Ingredients-Fragrance Inventory_v2.csv")

df_sales = pd.read_csv(r"C:\Users\letic\OneDrive\Documentos\ProjetoFinal-Grupo6\base\cosmetics_sales_data.csv")

df_cosmetics = pd.read_csv(r"C:\Users\letic\OneDrive\Documentos\ProjetoFinal-Grupo6\base\cosmetics.csv")

df_skin_products = pd.read_csv(r"C:\Users\letic\OneDrive\Documentos\ProjetoFinal-Grupo6\base\skincare_products_clean.csv")