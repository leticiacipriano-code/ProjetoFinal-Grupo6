-- =============================================
-- Glow & Co. — Inicialização dos Schemas
-- Executado automaticamente pelo Docker na
-- primeira inicialização do PostgreSQL.
-- =============================================

-- Camada Raw (dados brutos como vieram da fonte)
CREATE SCHEMA IF NOT EXISTS raw;

-- Camada Silver (dados limpos e tipados)
CREATE SCHEMA IF NOT EXISTS silver;

-- Camada Gold (modelo estrela, pronto para BI)
CREATE SCHEMA IF NOT EXISTS gold;

-- Concede privilégios ao usuário da aplicação
GRANT ALL PRIVILEGES ON SCHEMA raw    TO glow;
GRANT ALL PRIVILEGES ON SCHEMA silver TO glow;
GRANT ALL PRIVILEGES ON SCHEMA gold   TO glow;
