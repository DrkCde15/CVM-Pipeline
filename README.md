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
├── carregamento_db.py       # Camada Gold (agregações de negócio)
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

## Arquitetura Medalhão

Cada script do pipeline corresponde a uma camada da arquitetura medalhão:

| Camada | Script | Schema | Descrição |
| --- | --- | --- | --- |
| Bronze | `create_uc_tables.py` | `main.cvm.*` | Dados crus carregados dos CSVs |
| Silver | `validate_clean_data.py` | `main.cvm.*` | Dados limpos (dedup, CNPJ padronizado, trim) |
| Gold | `carregamento_db.py` | `main.cvm_gold.*` | Agregações de negócio prontas para consumo |

### Bronze – `main.cvm.*`

Tabelas carregadas diretamente dos CSVs pelo `create_uc_tables.py`:

| Tabela | Descrição |
| --- | --- |
| `main.cvm.cadastro` | Cadastro de fundos (41 colunas) |
| `main.cvm.inf_diario` | Informe diário (patrimônio, quota, cotistas) |
| `main.cvm.cda_pl` | Patrimônio líquido por fundo |
| `main.cvm.cda_fiim` | Informações do gestor/instrumentos |
| `main.cvm.cda_blc_1` a `cda_blc_8` | Blocos de balanço (títulos públicos, cotas, derivativos, etc.) |

### Silver – Limpeza

```python
from validate_clean_data import validar_e_limpar_tudo

# Validar e limpar todas as tabelas (sobrescreve com dados limpos)
validar_e_limpar_tudo()

# Apenas validar (sem alterar dados)
validar_e_limpar_tudo(aplicar_limpeza=False)
```

A limpeza remove duplicatas (chave de negócio), faz trim de strings,
padroniza CNPJ (apenas dígitos) e converte strings vazias em NULL.

### Gold – Agregações (`main.cvm_gold.*`)

```python
from carregamento_db import carregar_gold

# Criar todas as 4 tabelas Gold
carregar_gold()

# Ou criar apenas tabelas específicas
carregar_gold(nomes=["patrimonio_por_classe"])
```

Tabelas Gold:

| Tabela | Descrição |
| --- | --- |
| `main.cvm_gold.patrimonio_por_classe` | Patrimônio, captação e resgate por classe de fundo e dia |
| `main.cvm_gold.top_fundos_patrimonio` | Top 100 fundos por patrimônio (última data) |
| `main.cvm_gold.captacao_resgate_mensal` | Captação, resgate e fluxo líquido por mês |
| `main.cvm_gold.fundos_ativos` | Fundos em funcionamento com info cadastral e patrimônio |

## Fluxo completo do pipeline

```
1. download_cvm_data.py      → Bronze: baixa e descompacta os CSVs da CVM para data/
2. create_uc_tables.py       → Bronze: carrega os CSVs como tabelas em main.cvm.*
3. validate_clean_data.py    → Silver: limpa as tabelas main.cvm.* (dedup, CNPJ, trim)
4. carregamento_db.py        → Gold: cria agregações em main.cvm_gold.*
```

## Fonte dos dados

- Portal: http://dados.cvm.gov.br/
- Informe Diário: `https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_{YYYYMM}.zip`
- Cadastro: `https://dados.cvm.gov.br/dados/FI/CAD/DADOS/cad_fi.csv`
- CDA: `https://dados.cvm.gov.br/dados/FI/DOC/CDA/DADOS/cda_fi_{YYYYMM}.zip`
