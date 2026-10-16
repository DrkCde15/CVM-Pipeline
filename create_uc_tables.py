"""
Criação de Tabelas no Unity Catalog a partir dos CSVs da CVM
=============================================================
Lê os CSVs extraídos pelo download_cvm_data.py e cria tabelas gerenciadas
no Unity Catalog (schema main.cvm).

Tabelas criadas:
  - main.cvm.cadastro       – Cadastro de fundos (CSV único)
  - main.cvm.inf_diario     – Informe diário (múltiplos CSVs mensais)
  - main.cvm.cda_pl         – Patrimônio líquido
  - main.cvm.cda_fiim       – Informações do gestor/instrumentos
  - main.cvm.cda_blc_1..8   – Blocos de balanço

Uso:
  from create_uc_tables import criar_tabelas
  criar_tabelas()  # cria/ sobrescreve todas as tabelas
"""

from pathlib import Path
from pyspark.sql import SparkSession

# ── Configurações ────────────────────────────────────────────────────────────

# Pasta base dos CSVs (mesma do download_cvm_data.py)
BASE_DIR = Path(__file__).parent / "data"

# Catálogo e schema no Unity Catalog
CATALOG = "main"
SCHEMA  = "cvm"
FULL_SCHEMA = f"{CATALOG}.{SCHEMA}"

# Opções comuns de leitura de CSV
CSV_OPTIONS = dict(
    header=True,
    sep=";",
    encoding="iso-8859-1",
    inferSchema=True,
)

# Definição das tabelas: nome -> (subpasta, padrão de arquivo)
TABELAS = {
    "cadastro":   ("cadastro",   "cad_fi.csv"),
    "inf_diario": ("inf_diario", "inf_diario_fi_*.csv"),
    "cda_pl":     ("cda",        "cda_fi_PL_*.csv"),
    "cda_fiim":   ("cda",        "cda_fiim_*.csv"),
}

# BLC 1 a 8 seguem o mesmo padrão
for n in range(1, 9):
    TABELAS[f"cda_blc_{n}"] = ("cda", f"cda_fi_BLC_{n}_*.csv")


# ── Funções ──────────────────────────────────────────────────────────────────

def _spark() -> SparkSession:
    """Retorna a sessão Spark ativa."""
    return SparkSession.builder.getOrCreate()


def criar_schema():
    """Cria o schema no Unity Catalog se não existir."""
    spark = _spark()
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {FULL_SCHEMA}")
    print(f"  ✓ Schema `{FULL_SCHEMA}` garantido")


def ler_csv(subpasta: str, padrao: str):
    """Lê um ou múltiplos CSVs de uma subpasta e retorna um DataFrame."""
    spark = _spark()
    caminho = f"file://{BASE_DIR / subpasta / padrao}"
    df = spark.read.csv(caminho, **CSV_OPTIONS)
    return df


def criar_tabela(nome: str, modo: str = "overwrite"):
    """Cria ou sobrescreve uma tabela no Unity Catalog a partir dos CSVs."""
    subpasta, padrao = TABELAS[nome]
    tabela_full = f"{FULL_SCHEMA}.{nome}"

    print(f"  Criando {tabela_full} ...")
    spark = _spark()
    df = ler_csv(subpasta, padrao)
    df.write.mode(modo).saveAsTable(tabela_full)
    n_linhas = spark.table(tabela_full).count()
    print(f"    ✓ {n_linhas:,} linhas, {len(df.columns)} colunas")
    return n_linhas


def criar_tabelas(nomes=None, modo: str = "overwrite"):
    """
    Cria todas (ou um subconjunto) das tabelas da CVM no Unity Catalog.

    Parâmetros:
      nomes : lista de nomes de tabela (None = todas)
      modo  : 'overwrite' (padrão) ou 'append'
    """
    print("=" * 60)
    print("  Criação de Tabelas CVM no Unity Catalog")
    print("=" * 60)

    # 1. Garantir schema
    criar_schema()

    # 2. Determinar quais tabelas criar
    alvos = nomes if nomes else list(TABELAS.keys())

    # 3. Criar cada tabela
    print(f"\n  Criando {len(alvos)} tabela(s) ...\n")
    resumo = {}
    for nome in alvos:
        try:
            n = criar_tabela(nome, modo=modo)
            resumo[nome] = n
        except Exception as e:
            print(f"    ✗ Erro ao criar {nome}: {e}")
            resumo[nome] = 0

    # 4. Resumo final
    print("\n" + "=" * 60)
    print("  Resumo:")
    print(f"  {'Tabela':<20} {'Linhas':>12}")
    print(f"  {'-'*20} {'-'*12}")
    for nome, n in resumo.items():
        print(f"  {nome:<20} {n:>12,}")
    print("=" * 60)

    return resumo


# ── Execução ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    criar_tabelas()
