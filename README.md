# Projeto de Modelagem Dimensional e ETL

Entrega para a disciplina de Banco de Dados 2.
O objetivo foi simular um ambiente de Engenharia de Dados transformando um banco transacional (OLTP) em um Data Warehouse (OLAP).

## 🛠 Tecnologias Utilizadas
- **PostgreSQL**: Banco de dados relacional.
- **Python (Pandas/SQLAlchemy)**: Para carga inicial dos dados brutos.
- **PL/pgSQL**: Para automação via Stored Procedures e Triggers.

## 📋 Estrutura do Projeto
1. **Carga Inicial**: Script Python que lê arquivos CSV e popula o schema `public`.
2. **Camada de Preparação (View)**: Tratamento de dados (remoção de aspas, correção de formatação decimal).
3. **ETL (Stored Procedure)**: Processo que alimenta as tabelas Fato e Dimensão no schema `dw`.
4. **Auditoria (Trigger)**: Monitoramento de novas vendas em tempo real.

## 🚀 Como executar
1. Rode o script `1_carga_inicial.py` para popular a base.
2. Execute o script `2_script_dw_completo.sql` no seu cliente SQL (DBeaver/pgAdmin).
3. Chame a procedure: `CALL dw.sp_carga_dw();`
