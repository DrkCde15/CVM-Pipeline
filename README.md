# CVM-Pipeline

Pipeline para baixar, descompactar e carregar dados públicos da **CVM** (Comissão de Valores Mobiliários) no Unity Catalog da Databricks.

## Recursos da CVM

| Recurso | Formato | Descrição |
| --- | --- | --- |
| Informe Diário | ZIP → CSV | Informes diários de fundos de investimento (patrimônio, quota, captações e resgates) |
| Informação Cadastral | CSV | Cadastro de fundos (CNPJ, denominação, situação, classe, gestor, etc.) |
| CDA | ZIP → CSVs | Demonstrações Analíticas – blocos de balanço (BLC 1-8), PL e FIIM |

Os dados são públicos e **não exigem autenticação**.

## Estrutura do projeto

```
CVM-Pipeline/
├── download_cvm_data.py     # Download e descompactação dos ZIPs da CVM
├── create_uc_tables.py      # Carga inicial das tabelas no Unity Catalog
├── validate_clean_data.py   # Validação e limpeza das tabelas
├── carregamento_db.py       # Arquitetura medalhão (Bronze → Silver → Gold)
├── README.md
├── .gitignore
└── data/                    # CSVs baixados (ignorados pelo git)
    ├── inf_diario/
    ├── cadastro/
    └── cda/
```

## Como usar

```python
from download_cvm_data import executar_pipeline

# Baixar informes diários de 2024, cadastro e CDA de 2024
executar_pipeline(
    baixar_inf_diario=True, mes_inicio="202401", mes_fim="202412",
    baixar_cadastro=True,
    baixar_cda=True, cda_mes_inicio="202401", cda_mes_fim="202412",
)
```

Os CSVs são salvos em `data/` com subpastas separadas por tipo de recurso.

### Criar tabelas no Unity Catalog

```python
from create_uc_tables import criar_tabelas

# Criar todas as 12 tabelas (sobrescreve se já existirem)
criar_tabelas()

# Ou criar apenas tabelas específicas
criar_tabelas(nomes=["cadastro", "inf_diario"])
```

## Tabelas no Unity Catalog

### Carga inicial (`main.cvm.*`)

Tabelas carregadas diretamente dos CSVs pelo `create_uc_tables.py`:

| Tabela | Descrição |
| --- | --- |
| `main.cvm.cadastro` | Cadastro de fundos (41 colunas) |
| `main.cvm.inf_diario` | Informe diário (patrimônio, quota, cotistas) |
| `main.cvm.cda_pl` | Patrimônio líquido por fundo |
| `main.cvm.cda_fiim` | Informações do gestor/instrumentos |
| `main.cvm.cda_blc_1` a `cda_blc_8` | Blocos de balanço (títulos públicos, cotas, derivativos, etc.) |

### Arquitetura Medalhão (`carregamento_db.py`)

| Camada | Schema | Descrição |
| --- | --- | --- |
| Bronze | `main.cvm_bronze.*` | Dados crus dos CSVs com metadados (_source_file, _load_date) |
| Silver | `main.cvm_silver.*` | Dados limpos (dedup, CNPJ padronizado, trim) com MERGE incremental |
| Gold | `main.cvm_gold.*` | Agregações de negócio prontas para consumo |

Tabelas Gold:

| Tabela | Descrição |
| --- | --- |
| `main.cvm_gold.patrimonio_por_classe` | Patrimônio, captação e resgate por classe de fundo e dia |
| `main.cvm_gold.top_fundos_patrimonio` | Top 100 fundos por patrimônio (última data) |
| `main.cvm_gold.captacao_resgate_mensal` | Captação, resgate e fluxo líquido por mês |
| `main.cvm_gold.fundos_ativos` | Fundos em funcionamento com info cadastral e patrimônio |

### Validar e limpar dados

```python
from validate_clean_data import validar_e_limpar_tudo

# Validar e limpar todas as tabelas (sobrescreve com dados limpos)
validar_e_limpar_tudo()

# Apenas validar (sem alterar dados)
validar_e_limpar_tudo(aplicar_limpeza=False)
```

A limpeza remove duplicatas (chave de negócio), faz trim de strings,
padroniza CNPJ (apenas dígitos) e converte strings vazias em NULL.

### Carregar medalhão

```python
from carregamento_db import carregar_tudo

# Executar Bronze → Silver → Gold
carregar_tudo()

# Ou executar camadas individualmente
from carregamento_db import carregar_bronze, carregar_silver, carregar_gold
carregar_bronze()   # CSVs → Bronze (overwrite)
carregar_silver()   # Bronze → Silver (MERGE incremental)
carregar_gold()     # Silver → Gold (agregações)
```

## Fluxo completo do pipeline

```
1. download_cvm_data.py     → baixa e descompacta os CSVs da CVM para data/
2. carregamento_db.py       → carrega os dados em 3 camadas:
   ├── Bronze  (overwrite)   → dados crus com metadados
   ├── Silver  (MERGE)        → limpos e deduplicados incremental
   └── Gold    (overwrite)   → agregações de negócio
```

## Fonte dos dados

- Portal: http://dados.cvm.gov.br/
- Informe Diário: `https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_{YYYYMM}.zip`
- Cadastro: `https://dados.cvm.gov.br/dados/FI/CAD/DADOS/cad_fi.csv`
- CDA: `https://dados.cvm.gov.br/dados/FI/DOC/CDA/DADOS/cda_fi_{YYYYMM}.zip`
