"""
Arquitetura Medalhão – Carregamento dos Dados da CVM
=====================================================
Implementa as 3 camadas do padrão medalhão no Unity Catalog:

  Bronze  → dados crus dos CSVs (com metadados de origem)
  Silver  → dados limpos (dedup, CNPJ padronizado, trim, MERGE incremental)
  Gold    → agregações de negócio (patrimônio por classe, captação, top fundos)

Schemas criados:
  main.cvm.bronze.*   – raw
  main.cvm.silver.*   – limpo
  main.cvm.gold.*     – pronto para consumo

Uso:
  from load_medallion import carregar_bronze, carregar_silver, carregar_gold
  carregar_bronze()   # Bronze: CSVs → tabelas raw
  carregar_silver()   # Silver: Bronze → limpo com MERGE
  carregar_gold()     # Gold: Silver → agregações
"""

from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from delta.tables import DeltaTable

# ── Configurações ────────────────────────────────────────────────────────────

BASE_DIR = Path(__file__).parent / "data"
CATALOG = "main"
SCHEMA_BASE = "cvm"

CSV_OPTIONS = dict(
    header=True,
    sep=";",
    encoding="iso-8859-1",
    inferSchema=True,
)

# Definição das tabelas: nome → (subpasta, padrão de arquivo, chave_de_negocio)
TABELAS = {
    "cadastro":   ("cadastro",   "cad_fi.csv",         ["CNPJ_FUNDO"]),
    "inf_diario": ("inf_diario", "inf_diario_fi_*.csv", ["CNPJ_FUNDO_CLASSE", "DT_COMPTC"]),
    "cda_pl":     ("cda",        "cda_fi_PL_*.csv",     ["CNPJ_FUNDO_CLASSE", "DT_COMPTC"]),
    "cda_fiim":   ("cda",        "cda_fiim_*.csv",      ["CNPJ_FUNDO_CLASSE", "DT_COMPTC"]),
}
for n in range(1, 9):
    TABELAS[f"cda_blc_{n}"] = ("cda", f"cda_fi_BLC_{n}_*.csv", ["CNPJ_FUNDO_CLASSE", "DT_COMPTC"])

# Colunas de CNPJ para padronização (apenas dígitos)
COLUNAS_CNPJ = [
    "CNPJ_FUNDO", "CNPJ_FUNDO_CLASSE", "CNPJ_ADMIN", "CNPJ_AUDITOR",
    "CNPJ_CUSTODIANTE", "CPF_CNPJ_GESTOR", "CPF_CNPJ_EMISSOR",
    "CNPJ_FUNDO_CLASSE_COTA", "CNPJ_EMISSOR",
]

# ── Funções utilitárias ──────────────────────────────────────────────────────

def _spark() -> SparkSession:
    return SparkSession.builder.getOrCreate()

def _ler_csv(subpasta: str, padrao: str):
    """Lê CSVs com glob e adiciona metadados de origem."""
    spark = _spark()
    caminho = f"file://{BASE_DIR / subpasta / padrao}"
    df = spark.read.csv(caminho, **CSV_OPTIONS)
    # Adiciona metadados Bronze
    df = df.withColumn("_source_file", F.lit(padrao)) \
           .withColumn("_load_date", F.current_timestamp())
    return df

def _garantir_schema(camada: str):
    """Cria o schema da camada se não existir."""
    spark = _spark()
    schema_name = f"{CATALOG}.{SCHEMA_BASE}_{camada}"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
    print(f"  ✓ Schema `{schema_name}` garantido")
    return schema_name

def _limpar_df(df):
    """Aplica limpeza: trim strings, vazias→NULL, CNPJ→dígitos."""
    for col in df.columns:
        if col.startswith("_"):
            continue
        if isinstance(df.schema[col].dataType, StringType):
            df = df.withColumn(col, F.trim(F.col(col)))
            df = df.withColumn(col, F.when(F.col(col) == "", F.lit(None)).otherwise(F.col(col)))
    for col in df.columns:
        if col in COLUNAS_CNPJ:
            df = df.withColumn(
                col,
                F.when(F.col(col).isNotNull(), F.regexp_replace(F.col(col), r"[^0-9]", ""))
                .otherwise(F.col(col))
            )
    return df

# ── BRONZE ───────────────────────────────────────────────────────────────────

def carregar_bronze(nomes=None) -> dict:
    """
    Carrega os CSVs crus para main.cvm_bronze.* com metadados (_source_file, _load_date).
    Modo overwrite — sempre recria com os CSVs mais recentes.
    """
    print("=" * 60)
    print("  BRONZE – Carga de dados crus")
    print("=" * 60)

    schema = _garantir_schema("bronze")
    alvos = nomes if nomes else list(TABELAS.keys())
    resumo = {}

    for nome in alvos:
        subpasta, padrao, _ = TABELAS[nome]
        tabela_full = f"{schema}.{nome}"
        print(f"\n  Bronze: {tabela_full} ...")
        try:
            df = _ler_csv(subpasta, padrao)
            df.write.mode("overwrite").saveAsTable(tabela_full)
            n = df.count()
            print(f"    ✓ {n:,} linhas, {len(df.columns)} colunas")
            resumo[nome] = n
        except Exception as e:
            print(f"    ✗ Erro: {e}")
            resumo[nome] = 0

    print("\n" + "=" * 60)
    return resumo

# ── SILVER ───────────────────────────────────────────────────────────────────

def carregar_silver(nomes=None) -> dict:
    """
    Lê da camada Bronze, aplica limpeza e faz MERGE incremental para main.cvm_silver.*.
    Se a tabela Silver não existe, cria. Se existe, atualiza apenas registros novos/alterados.
    """
    print("=" * 60)
    print("  SILVER – Limpeza e MERGE incremental")
    print("=" * 60)

    bronze_schema = f"{CATALOG}.{SCHEMA_BASE}_bronze"
    silver_schema = _garantir_schema("silver")
    alvos = nomes if nomes else list(TABELAS.keys())
    resumo = {}

    for nome in alvos:
        subpasta, padrao, chaves = TABELAS[nome]
        bronze_table = f"{bronze_schema}.{nome}"
        silver_table = f"{silver_schema}.{nome}"
        print(f"\n  Silver: {silver_table} ...")
        try:
            # 1. Ler Bronze e limpar
            df_bronze = _spark().table(bronze_table)
            df_limpo = _limpar_df(df_bronze)

            # 2. Remover duplicatas da chave (manter primeira ocorrência)
            from pyspark.sql.window import Window
            w = Window.partitionBy(*chaves).orderBy(F.monotonically_increasing_id())
            df_limpo = df_limpo.withColumn("_rn", F.row_number().over(w)) \
                               .filter(F.col("_rn") == 1) \
                               .drop("_rn", "_source_file", "_load_date")

            # 3. MERGE incremental
            if _spark().catalog.tableExists(silver_table):
                # Tabela existe → MERGE
                delta_table = DeltaTable.forName(_spark(), silver_table)
                condicao = " AND ".join([f"target.{k} = source.{k}" for k in chaves])

                delta_table.alias("target").merge(
                    df_limpo.alias("source"), condicao
                ).whenMatchedUpdateAll() \
                 .whenNotMatchedInsertAll() \
                 .execute()

                n_antes = _spark().table(silver_table).count()
                print(f"    ✓ MERGE concluído – {n_antes:,} linhas na tabela")
                resumo[nome] = n_antes
            else:
                # Tabela não existe → criar
                df_limpo.write.mode("overwrite").saveAsTable(silver_table)
                n = df_limpo.count()
                print(f"    ✓ Tabela criada – {n:,} linhas")
                resumo[nome] = n

        except Exception as e:
            print(f"    ✗ Erro: {e}")
            resumo[nome] = 0

    print("\n" + "=" * 60)
    return resumo

# ── GOLD ─────────────────────────────────────────────────────────────────────

# Queries SQL das tabelas Gold
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
        FROM main.cvm_silver.inf_diario i
        LEFT JOIN main.cvm_silver.cadastro c
            ON i.CNPJ_FUNDO_CLASSE = c.CNPJ_FUNDO
        WHERE i.VL_PATRIM_LIQ IS NOT NULL
        GROUP BY c.CLASSE, i.DT_COMPTC
        ORDER BY i.DT_COMPTC, c.CLASSE
    """,

    # 2. Top 100 fundos por patrimônio (última data disponível)
    "top_fundos_patrimonio": """
        WITH ultima_data AS (
            SELECT MAX(DT_COMPTC) AS dt FROM main.cvm_silver.inf_diario
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
        FROM main.cvm_silver.inf_diario i
        LEFT JOIN main.cvm_silver.cadastro c
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
        FROM main.cvm_silver.inf_diario
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
        FROM main.cvm_silver.cadastro c
        LEFT JOIN (
            SELECT CNPJ_FUNDO_CLASSE, VL_PATRIM_LIQ, DT_COMPTC,
                   ROW_NUMBER() OVER (PARTITION BY CNPJ_FUNDO_CLASSE ORDER BY DT_COMPTC DESC) AS rn
            FROM main.cvm_silver.cda_pl
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

    silver_schema = f"{CATALOG}.{SCHEMA_BASE}_silver"
    gold_schema = _garantir_schema("gold")
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

# ── Orquestração ──────────────────────────────────────────────────────────────

def carregar_tudo() -> None:
    """
    Executa o fluxo completo: Bronze → Silver → Gold.
    """
    print("╔" + "═" * 58 + "╗")
    print("║  Arquitetura Medalhão CVM – Bronze → Silver → Gold       ║")
    print("╚" + "═" * 58 + "╝")

    print("\n>>> CAMADA BRONZE\n")
    carregar_bronze()

    print("\n>>> CAMADA SILVER\n")
    carregar_silver()

    print("\n>>> CAMADA GOLD\n")
    carregar_gold()

    print("\n✅ Medalhão completo!")
    print(f"   main.cvm_bronze.*  – dados crus")
    print(f"   main.cvm_silver.*  – dados limpos")
    print(f"   main.cvm_gold.*    – agregações")


# ── Execução ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    carregar_tudo()
