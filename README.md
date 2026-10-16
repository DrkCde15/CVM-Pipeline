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
├── download_cvm_data.py   # Script de download e descompactação
├── create_uc_tables.py    # Script de criação das tabelas no Unity Catalog
├── README.md
├── .gitignore
└── data/                  # CSVs baixados (ignorados pelo git)
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

Os dados são carregados no schema `main.cvm` com as seguintes tabelas:

| Tabela | Descrição |
| --- | --- |
| `main.cvm.cadastro` | Cadastro de fundos (41 colunas) |
| `main.cvm.inf_diario` | Informe diário (patrimônio, quota, cotistas) |
| `main.cvm.cda_pl` | Patrimônio líquido por fundo |
| `main.cvm.cda_fiim` | Informações do gestor/instrumentos |
| `main.cvm.cda_blc_1` a `cda_blc_8` | Blocos de balanço (títulos públicos, cotas, derivativos, etc.) |

## Fonte dos dados

- Portal: http://dados.cvm.gov.br/
- Informe Diário: `https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_{YYYYMM}.zip`
- Cadastro: `https://dados.cvm.gov.br/dados/FI/CAD/DADOS/cad_fi.csv`
- CDA: `https://dados.cvm.gov.br/dados/FI/DOC/CDA/DADOS/cda_fi_{YYYYMM}.zip`
