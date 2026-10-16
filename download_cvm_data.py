"""
Pipeline de Download de Dados da CVM
=====================================
Baixa, descompacta e organiza dados públicos da CVM (Comissão de Valores Mobiliários):
  1. Informe Diário (fundos de investimento)  – ZIP contendo CSV
  2. Informação Cadastral (cadastro de fundos) – CSV
  3. CDA (Demonstrações Analíticas)            – ZIP contendo CSVs

Fonte: http://dados.cvm.gov.br/
Não exige autenticação para download público.
"""

import os
import io
import zipfile
import requests
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pathlib import Path
from typing import Optional

# ── Configurações ────────────────────────────────────────────────────────────

# Pasta base onde os dados serão salvos (relativa à localização do script)
BASE_DIR = Path(__file__).parent / "data"

# URLs base dos recursos da CVM
URL_INF_DIARIO = "https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_{period}.zip"
URL_CAD_FI    = "https://dados.cvm.gov.br/dados/FI/CAD/DADOS/cad_fi.csv"
URL_CDA       = "https://dados.cvm.gov.br/dados/FI/DOC/CDA/DADOS/cda_fi_{period}.zip"

# Subpastas para cada tipo de dado
SUBPASTAS = {
    "inf_diario":  "inf_diario",
    "cadastro":    "cadastro",
    "cda":         "cda",
}

# Tempo limite para cada download (segundos)
TIMEOUT = 120

# Headers para evitar bloqueio por user-agent
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; CVM-Pipeline/1.0)"
}


# ── Funções utilitárias ──────────────────────────────────────────────────────

def garantir_pasta(tipo: str) -> Path:
    """Cria (se necessário) e retorna a pasta de destino para o tipo de dado."""
    pasta = BASE_DIR / SUBPASTAS[tipo]
    pasta.mkdir(parents=True, exist_ok=True)
    return pasta


def baixar_arquivo(url: str) -> bytes:
    """Faz o download de um arquivo e retorna seu conteúdo em bytes."""
    resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
    resp.raise_for_status()
    return resp.content


def salvar_e_descompactar(conteudo: bytes, destino: Path, arquivo_zip: str) -> list[Path]:
    """Salva o ZIP e extrai seu conteúdo na pasta de destino.
    Retorna a lista de arquivos extraídos."""
    zip_path = destino / arquivo_zip
    zip_path.write_bytes(conteudo)

    arquivos_extraidos = []
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(destino)
        arquivos_extraidos = [destino / name for name in zf.namelist()]

    # Remove o ZIP após extrair (opcional – comente se quiser manter)
    zip_path.unlink()

    return arquivos_extraidos


def salvar_csv(conteudo: bytes, destino: Path, nome_arquivo: str) -> Path:
    """Salva um arquivo CSV/TXT diretamente."""
    path = destino / nome_arquivo
    path.write_bytes(conteudo)
    return path


# ── Downloads por recurso ────────────────────────────────────────────────────

def download_inf_diario(mes_inicio: str, mes_fim: str) -> list[Path]:
    """
    Baixa o Informe Diário de fundos para um intervalo de meses.
    Formato do período: YYYYMM (ex: 202401 para janeiro/2024).
    """
    destino = garantir_pasta("inf_diario")
    arquivos = []

    inicio = datetime.strptime(mes_inicio, "%Y%m")
    fim = datetime.strptime(mes_fim, "%Y%m")

    atual = inicio
    while atual <= fim:
        periodo = atual.strftime("%Y%m")
        url = URL_INF_DIARIO.format(period=periodo)
        print(f"  [Inf Diário] Baixando {periodo} ...")
        try:
            conteudo = baixar_arquivo(url)
            extraidos = salvar_e_descompactar(conteudo, destino, f"inf_diario_fi_{periodo}.zip")
            arquivos.extend(extraidos)
            print(f"    ✓ {len(extraidos)} arquivo(s) extraído(s)")
        except requests.HTTPError as e:
            print(f"    ✗ Falha ao baixar {periodo}: {e}")
        atual += relativedelta(months=1)

    return arquivos


def download_cadastro() -> Path | None:
    """Baixa o arquivo de cadastro de fundos (CSV único, sem compactação)."""
    destino = garantir_pasta("cadastro")
    print("  [Cadastro] Baixando cad_fi.csv ...")
    try:
        conteudo = baixar_arquivo(URL_CAD_FI)
        path = salvar_csv(conteudo, destino, "cad_fi.csv")
        print(f"    ✓ Arquivo salvo: {path}")
        return path
    except requests.HTTPError as e:
        print(f"    ✗ Falha ao baixar cadastro: {e}")
        return None


def download_cda(mes_inicio: str, mes_fim: str) -> list[Path]:
    """
    Baixa as Demonstrações Analíticas (CDA) por mês.
    Formato do período: YYYYMM (ex: 202401 para janeiro/2024).
    """
    destino = garantir_pasta("cda")
    arquivos = []

    inicio = datetime.strptime(mes_inicio, "%Y%m")
    fim = datetime.strptime(mes_fim, "%Y%m")

    atual = inicio
    while atual <= fim:
        periodo = atual.strftime("%Y%m")
        url = URL_CDA.format(period=periodo)
        print(f"  [CDA] Baixando {periodo} ...")
        try:
            conteudo = baixar_arquivo(url)
            extraidos = salvar_e_descompactar(conteudo, destino, f"cda_fi_{periodo}.zip")
            arquivos.extend(extraidos)
            print(f"    ✓ {len(extraidos)} arquivo(s) extraído(s)")
        except requests.HTTPError as e:
            print(f"    ✗ Falha ao baixar CDA {periodo}: {e}")
        atual += relativedelta(months=1)

    return arquivos


# ── Orquestração ─────────────────────────────────────────────────────────────

def executar_pipeline(
    baixar_inf_diario: bool = True,
    mes_inicio: str = "202401",
    mes_fim: str = "202412",
    baixar_cadastro: bool = True,
    baixar_cda: bool = True,
    cda_mes_inicio: str = "202401",
    cda_mes_fim: str = "202412",
):
    """
    Executa o pipeline completo de download da CVM.

    Parâmetros:
      baixar_inf_diario  : se True, baixa Informes Diários no intervalo de meses
      mes_inicio/mes_fim : intervalo no formato YYYYMM
      baixar_cadastro    : se True, baixa o cadastro de fundos
      baixar_cda         : se True, baixa as CDAs no intervalo de meses
      cda_mes_inicio/fim : intervalo no formato YYYYMM para CDA
    """
    print("=" * 60)
    print("  Pipeline de Download de Dados da CVM")
    print("=" * 60)
    print(f"  Pasta base: {BASE_DIR}\n")

    if baixar_inf_diario:
        print("\n── Informe Diário ──")
        download_inf_diario(mes_inicio, mes_fim)

    if baixar_cadastro:
        print("\n── Informação Cadastral ──")
        download_cadastro()

    if baixar_cda:
        print("\n── CDA (Demonstrações Analíticas) ──")
        download_cda(cda_mes_inicio, cda_mes_fim)

    print("\n" + "=" * 60)
    print("  Pipeline concluído!")
    print(f"  Dados disponíveis em: {BASE_DIR}")
    print("=" * 60)


# ── Execução ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # Exemplo: baixar informes diários de 2024, cadastro completo e CDA de 2024
    executar_pipeline(
        baixar_inf_diario=True,
        mes_inicio="202401",
        mes_fim="202412",
        baixar_cadastro=True,
        baixar_cda=True,
        cda_mes_inicio="202401",
        cda_mes_fim="202412",
    )
