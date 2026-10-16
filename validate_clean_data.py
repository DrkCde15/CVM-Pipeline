"""
Validação e Limpeza dos Dados da CVM no Unity Catalog
======================================================
Inspeciona as tabelas em `main.cvm.*` procurando:
  - Linhas duplicadas (chaves de negócio repetidas)
  - Colunas com valores nulos
  - Strings com espaços em branco ou vazias
  - CNPJ em formato inconsistente

Em seguida aplica a limpeza:
  - Remove duplicatas (mantendo a primeira ocorrência)
  - Faz trim de strings e converte strings vazias em NULL
  - Padroniza CNPJ removendo pontuação (apenas dígitos)
  - Sobrescreve a tabela limpa no Unity Catalog

Uso:
  from validate_clean_data import validar_e_limpar_tudo
  validar_e_limpar_tudo()  # valida e limpa todas as tabelas

  # Apenas validar (sem alterar dados):
  from validate_clean_data import validar_tabela
  validar_tabela("inf_diario")
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# ── Configurações ────────────────────────────────────────────────────────────

CATALOG = "main"
SCHEMA  = "cvm"
FULL_SCHEMA = f"{CATALOG}.{SCHEMA}"

# Chaves de negócio (colunas que identificam uma linha única) por tabela
CHAVES = {
    "cadastro":   ["CNPJ_FUNDO"],
    "inf_diario": ["CNPJ_FUNDO_CLASSE", "DT_COMPTC"],
    "cda_pl":     ["CNPJ_FUNDO_CLASSE", "DT_COMPTC"],
    "cda_fiim":   ["CNPJ_FUNDO_CLASSE", "DT_COMPTC"],
    "cda_blc_1":  ["CNPJ_FUNDO_CLASSE", "DT_COMPTC"],
    "cda_blc_2":  ["CNPJ_FUNDO_CLASSE", "DT_COMPTC"],
    "cda_blc_3":  ["CNPJ_FUNDO_CLASSE", "DT_COMPTC"],
    "cda_blc_4":  ["CNPJ_FUNDO_CLASSE", "DT_COMPTC"],
    "cda_blc_5":  ["CNPJ_FUNDO_CLASSE", "DT_COMPTC"],
    "cda_blc_6":  ["CNPJ_FUNDO_CLASSE", "DT_COMPTC"],
    "cda_blc_7":  ["CNPJ_FUNDO_CLASSE", "DT_COMPTC"],
    "cda_blc_8":  ["CNPJ_FUNDO_CLASSE", "DT_COMPTC"],
}

# Colunas de CNPJ para padronização (apenas dígitos)
COLUNAS_CNPJ = [
    "CNPJ_FUNDO",
    "CNPJ_FUNDO_CLASSE",
    "CNPJ_ADMIN",
    "CNPJ_AUDITOR",
    "CNPJ_CUSTODIANTE",
    "CPF_CNPJ_GESTOR",
    "CPF_CNPJ_EMISSOR",
    "CNPJ_FUNDO_CLASSE_COTA",
    "CNPJ_EMISSOR",
]


# ── Funções utilitárias ──────────────────────────────────────────────────────

def _spark() -> SparkSession:
    return SparkSession.builder.getOrCreate()


def _tabela_full(nome: str) -> str:
    return f"{FULL_SCHEMA}.{nome}"


# ── Validação ─────────────────────────────────────────────────────────────────

def validar_tabela(nome: str) -> dict:
    """
    Inspeciona uma tabela e retorna um relatório de qualidade:
      - total de linhas
      - duplicatas pela chave de negócio
      - colunas com valores nulos
      - colunas string com espaços em excesso ou valores vazios
    """
    spark = _spark()
    tabela_full = _tabela_full(nome)
    df = spark.table(tabela_full)
    total = df.count()
    colunas = df.columns
    chaves = CHAVES.get(nome, [])

    print(f"\n── {tabela_full} ──")
    print(f"  Total de linhas: {total:,}")

    relatorio = {"tabela": nome, "total": total, "duplicatas": 0, "nulos": {}, "strings_vazias": {}}

    # 1. Duplicatas
    if chaves:
        n_unicos = df.select(*chaves).distinct().count()
        duplicatas = total - n_unicos
        relatorio["duplicatas"] = duplicatas
        if duplicatas > 0:
            print(f"  ⚠ {duplicatas:,} duplicata(s) pela chave {chaves}")
        else:
            print(f"  ✓ Sem duplicatas pela chave {chaves}")
    else:
        print(f"  (sem chave definida para validação)")

    # 2. Nulos por coluna
    for col in colunas:
        n_nulos = df.filter(F.col(col).isNull()).count()
        if n_nulos > 0:
            pct = (n_nulos / total * 100) if total > 0 else 0
            relatorio["nulos"][col] = n_nulos
            if pct > 50:
                print(f"  ⚠ {col}: {n_nulos:,} nulos ({pct:.1f}%)")

    if not relatorio["nulos"]:
        print(f"  ✓ Sem colunas nulas")

    # 3. Strings vazias ou com espaços
    colunas_string = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
    for col in colunas_string:
        n_vazias = df.filter((F.col(col) == "") | (F.trim(F.col(col)) == "")).count()
        if n_vazias > 0:
            relatorio["strings_vazias"][col] = n_vazias
            print(f"  ⚠ {col}: {n_vazias:,} string(s) vazia(s)")

    if not relatorio["strings_vazias"]:
        print(f"  ✓ Sem strings vazias")

    return relatorio


def validar_tudo() -> list[dict]:
    """Valida todas as tabelas e retorna uma lista de relatórios."""
    print("=" * 60)
    print("  Validação de Dados CVM")
    print("=" * 60)
    resultados = []
    for nome in CHAVES:
        try:
            r = validar_tabela(nome)
            resultados.append(r)
        except Exception as e:
            print(f"  ✗ Erro ao validar {nome}: {e}")
            resultados.append({"tabela": nome, "erro": str(e)})
    print("\n" + "=" * 60)
    return resultados


# ── Limpeza ───────────────────────────────────────────────────────────────────

def limpar_tabela(nome: str, aplicar: bool = True) -> dict:
    """
    Aplica a limpeza em uma tabela:
      1. Trim de strings e conversão de vazias em NULL
      2. Padronização de colunas de CNPJ (apenas dígitos)
      3. Remoção de duplicatas pela chave de negócio

    Parâmetros:
      nome    : nome da tabela (sem o prefixo main.cvm.)
      aplicar : se True, sobrescreve a tabela; se False, apenas mostra o relatório
    """
    spark = _spark()
    tabela_full = _tabela_full(nome)
    df = spark.table(tabela_full)
    total_antes = df.count()
    chaves = CHAVES.get(nome, [])

    print(f"\n── Limpando {tabela_full} ──")
    print(f"  Linhas antes: {total_antes:,}")

    # 1. Trim strings + vazias -> NULL
    for col in df.columns:
        if isinstance(df.schema[col].dataType, StringType):
            df = df.withColumn(col, F.trim(F.col(col)))
            df = df.withColumn(
                col,
                F.when(F.col(col) == "", F.lit(None)).otherwise(F.col(col))
            )

    # 2. Padronizar CNPJ (remover pontuação, manter apenas dígitos)
    for col in df.columns:
        if col in COLUNAS_CNPJ:
            df = df.withColumn(
                col,
                F.when(F.col(col).isNotNull(), F.regexp_replace(F.col(col), r"[^0-9]", "")).otherwise(F.col(col))
            )

    # 3. Remover duplicatas
    if chaves:
        total_dups = df.count() - df.select(*chaves).distinct().count()
        if total_dups > 0:
            # Adiciona row_number para manter a primeira ocorrência
            from pyspark.sql.window import Window
            janela = Window.partitionBy(*chaves).orderBy(F.monotonically_increasing_id())
            df = df.withColumn("_rn", F.row_number().over(janela)).filter(F.col("_rn") == 1).drop("_rn")
            print(f"  Duplicatas removidas: {total_dups:,}")
        else:
            print(f"  ✓ Sem duplicatas")

    total_depois = df.count()
    print(f"  Linhas depois: {total_depois:,}")

    if aplicar:
        df.write.mode("overwrite").saveAsTable(tabela_full)
        print(f"  ✓ Tabela {tabela_full} sobrescrita com dados limpos")
    else:
        print(f"  (modo simulação – tabela não alterada)")

    return {"tabela": nome, "antes": total_antes, "depois": total_depois, "aplicado": aplicar}


def limpar_tudo(aplicar: bool = True) -> list[dict]:
    """Limpa todas as tabelas."""
    print("=" * 60)
    print("  Limpeza de Dados CVM")
    print("=" * 60)
    resultados = []
    for nome in CHAVES:
        try:
            r = limpar_tabela(nome, aplicar=aplicar)
            resultados.append(r)
        except Exception as e:
            print(f"  ✗ Erro ao limpar {nome}: {e}")
            resultados.append({"tabela": nome, "erro": str(e)})
    print("\n" + "=" * 60)
    return resultados


# ── Orquestração ──────────────────────────────────────────────────────────────

def validar_e_limpar_tudo(aplicar_limpeza: bool = True) -> None:
    """
    Executa o fluxo completo: valida todas as tabelas, aplica a limpeza,
    e valida novamente para confirmar.
    """
    print("╔" + "═" * 58 + "╗")
    print("║  Validação e Limpeza de Dados da CVM                       ║")
    print("╚" + "═" * 58 + "╝")

    # 1. Validação inicial
    print("\n>>> FASE 1: Validação inicial\n")
    validar_tudo()

    # 2. Limpeza
    if aplicar_limpeza:
        print("\n>>> FASE 2: Limpeza\n")
        limpar_tudo(aplicar=True)

        # 3. Validação final
        print("\n>>> FASE 3: Validação após limpeza\n")
        validar_tudo()
    else:
        print("\n>>> FASE 2: Limpeza (simulação)\n")
        limpar_tudo(aplicar=False)

    print("\n✅ Concluído!")


# ── Execução ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    validar_e_limpar_tudo(aplicar_limpeza=True)
