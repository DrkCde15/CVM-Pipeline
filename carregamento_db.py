"""
Camada Gold – Agregações de Negócio da CVM
==========================================
Cria tabelas Gold no Unity Catalog (main.cvm_gold.*) a partir dos dados
limpos (main.cvm.*) pelo validate_clean_data.py.

Tabelas Gold:
  - main.cvm_gold.patrimonio_por_classe  – patrimônio/captação por classe e dia
  - main.cvm_gold.top_fundos_patrimonio  – top 100 fundos por patrimônio
  - main.cvm_gold.captacao_resgate_mensal – captação/resgate por mês
  - main.cvm_gold.fundos_ativos          – fundos ativos com patrimônio

Uso:
  from carregamento_db import carregar_gold
  carregar_gold()  # cria todas as agregações
"""

from pyspark.sql import SparkSession

# ── Configurações ────────────────────────────────────────────────────────────

CATALOG = "main"
SCHEMA_BASE = "cvm"

# ── Funções utilitárias ──────────────────────────────────────────────────────

def _spark() -> SparkSession:
    return SparkSession.builder.getOrCreate()

def _garantir_schema():
    """Cria o schema gold se não existir."""
    spark = _spark()
    schema_name = f"{CATALOG}.{SCHEMA_BASE}_gold"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
    print(f"  ✓ Schema `{schema_name}` garantido")
    return schema_name

# ── GOLD ─────────────────────────────────────────────────────────────────────

# Queries SQL das tabelas Gold (lêem de main.cvm.* — tabelas limpas pelo validate_clean_data.py)
GOLD_QUERIES = {
    # 1. Patrimônio líquido total por classe de fundo e mês
    "patrimonio_por_classe": """
        SELECT
            c.CLASSE,
            i.DT_COMPTC,
            COUNT(DISTINCT i.CNPJ_FUNDO_CLASSE) AS qtd_fundos,
            SUM(i.VL_PATRIM_LIQ) AS vl_patrim_total,
            SUM(i.CAPTC_DIA) AS captacao_total,
            SUM(i.RESG_DIA) AS resgate_total,
            SUM(i.CAPTC_DIA - i.RESG_DIA) AS fluxo_liquido
        FROM main.cvm.inf_diario i
        LEFT JOIN main.cvm.cadastro c
            ON i.CNPJ_FUNDO_CLASSE = c.CNPJ_FUNDO
        WHERE i.VL_PATRIM_LIQ IS NOT NULL
        GROUP BY c.CLASSE, i.DT_COMPTC
        ORDER BY i.DT_COMPTC, c.CLASSE
    """,

    # 2. Top 100 fundos por patrimônio (última data disponível)
    "top_fundos_patrimonio": """
        WITH ultima_data AS (
            SELECT MAX(DT_COMPTC) AS dt FROM main.cvm.inf_diario
        )
        SELECT
            i.CNPJ_FUNDO_CLASSE,
            c.DENOM_SOCIAL,
            c.CLASSE,
            c.SIT,
            i.DT_COMPTC,
            i.VL_PATRIM_LIQ,
            i.VL_QUOTA,
            i.NR_COTST,
            RANK() OVER (ORDER BY i.VL_PATRIM_LIQ DESC) AS ranking
        FROM main.cvm.inf_diario i
        LEFT JOIN main.cvm.cadastro c
            ON i.CNPJ_FUNDO_CLASSE = c.CNPJ_FUNDO
        CROSS JOIN ultima_data ud
        WHERE i.DT_COMPTC = ud.dt
            AND i.VL_PATRIM_LIQ IS NOT NULL
        ORDER BY i.VL_PATRIM_LIQ DESC
        LIMIT 100
    """,

    # 3. Captação e resgate líquido por mês
    "captacao_resgate_mensal": """
        SELECT
            DATE_TRUNC('month', DT_COMPTC) AS mes,
            COUNT(DISTINCT CNPJ_FUNDO_CLASSE) AS qtd_fundos,
            SUM(CAPTC_DIA) AS captacao_total,
            SUM(RESG_DIA) AS resgate_total,
            SUM(CAPTC_DIA - RESG_DIA) AS fluxo_liquido,
            SUM(VL_PATRIM_LIQ) AS patrim_total
        FROM main.cvm.inf_diario
        WHERE CAPTC_DIA IS NOT NULL OR RESG_DIA IS NOT NULL
        GROUP BY DATE_TRUNC('month', DT_COMPTC)
        ORDER BY mes
    """,

    # 4. Fundos ativos com info cadastral
    "fundos_ativos": """
        SELECT
            c.CNPJ_FUNDO,
            c.DENOM_SOCIAL,
            c.CLASSE,
            c.SIT,
            c.DT_INI_ATIV,
            c.RENTAB_FUNDO,
            c.CONDOM,
            c.FUNDO_COTAS,
            c.TAXA_ADM,
            c.TAXA_PERFM,
            c.GESTOR,
            c.ADMIN,
            c.AUDITOR,
            p.VL_PATRIM_LIQ,
            p.DT_COMPTC AS dt_patrim
        FROM main.cvm.cadastro c
        LEFT JOIN (
            SELECT CNPJ_FUNDO_CLASSE, VL_PATRIM_LIQ, DT_COMPTC,
                   ROW_NUMBER() OVER (PARTITION BY CNPJ_FUNDO_CLASSE ORDER BY DT_COMPTC DESC) AS rn
            FROM main.cvm.cda_pl
        ) p ON c.CNPJ_FUNDO = p.CNPJ_FUNDO_CLASSE AND p.rn = 1
        WHERE c.SIT = 'EM FUNCIONAMENTO NORMAL'
    """,
}

def carregar_gold(nomes=None) -> dict:
    """
    Cria tabelas Gold (agregações) a partir da camada Silver.
    Sempre overwrite — são recriadas a cada execução com os dados mais recentes.
    """
    print("=" * 60)
    print("  GOLD – Agregações de negócio")
    print("=" * 60)

    gold_schema = _garantir_schema()
    alvos = nomes if nomes else list(GOLD_QUERIES.keys())
    resumo = {}
    spark = _spark()

    for nome in alvos:
        tabela_full = f"{gold_schema}.{nome}"
        print(f"\n  Gold: {tabela_full} ...")
        try:
            df = spark.sql(GOLD_QUERIES[nome])
            df.write.mode("overwrite").saveAsTable(tabela_full)
            n = df.count()
            print(f"    ✓ {n:,} linhas")
            resumo[nome] = n
        except Exception as e:
            print(f"    ✗ Erro: {e}")
            resumo[nome] = 0

    print("\n" + "=" * 60)
    return resumo

# ── Execução ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    carregar_gold()
