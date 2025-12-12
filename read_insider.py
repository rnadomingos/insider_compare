from datetime import datetime, date, time, timezone
from typing import Any
from sqlalchemy.types import String, DateTime, Float
from sqlalchemy.exc import SQLAlchemyError
from infra.oracle_database import engine
from sqlalchemy import Numeric, text
import pandas as pd
import glob
import os
import re


# ============================================================
#  Limpeza e Normalização de Telefones
# ============================================================
def clean_phone(value):
    if pd.isna(value):
        return None
    value = str(value)
    value = re.sub(r"\D", "", value)
    return value if value else None


# ============================================================
#  Normalização Completa do DataFrame
# ============================================================
def normalize_dataframe(df: pd.DataFrame):
    df = df.copy()

    # 1. Nomes das colunas: uppercase e sem espaços
    df.columns = [
    str(col).strip().upper().replace('"', '').replace("'", "")
    for col in df.columns
    ]

    # 2. Detectar colunas que parecem datas
    date_like_cols = [c for c in df.columns if "DATA" in c or "DATE" in c]

    for col in date_like_cols:
        df[col] = pd.to_datetime(df[col], errors="coerce", dayfirst=True)

    # 3. Detectar colunas que parecem telefone
    phone_like_cols = [
        c for c in df.columns
        if any(x in c for x in ["FONE", "TELEFONE", "CEL", "WHATS", "WHATSAPP"])
    ]

    upper_cols = [
        c for c in df.columns
        if any(x in c for x in ["NOME", "MARCA", "MODELO"])
    ]

    for col in upper_cols:  
        df[col] = df[col].astype(str).str.upper().str.strip()

    for col in phone_like_cols:
        df[col] = df[col].apply(clean_phone).astype("object")

    # 4. Garantir que todo campo não numérico e não data → string
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            continue
        if pd.api.types.is_numeric_dtype(df[col]):
            continue
        df[col] = df[col].astype(str).replace("nan", None).replace("None", "")

    return df


# ============================================================
#  Conversor de Datas
# ============================================================
def parse_date(value):
    if pd.isna(value):
        return None

    value = str(value).strip()

    formats = [
        "%d/%m/%Y",
        "%d/%m/%y",
        "%d/%m/%Y %H:%M",
        "%d/%m/%y %H:%M",
        "%Y-%m-%d",
        "%Y-%m-%d %H:%M",
        "%Y%m%d"
    ]

    for fmt in formats:
        try:
            return datetime.strptime(value, fmt)
        except:
            continue

    try:
        return pd.to_datetime(value, errors="coerce", dayfirst=True)
    except:
        return None


# ============================================================
#  Processa e lê arquivos de leads
# ============================================================
def process_all_leads(input_folder, prefix_file, output_folder=None, inbox_name=None, table_name=None):
    files = glob.glob(os.path.join(input_folder, f"{prefix_file}*.*"))

    if not files:
        print("Nenhum arquivo encontrado com prefixo lead_.")
        return

    for file_path in files:
        print(f"Processando: {file_path}")
        base = os.path.basename(file_path)

        # 1) Lê e limpa
        df_clean = read_and_clean_leads(file_path, inbox_name)
        df_clean = normalize_dataframe(df_clean)

        # 2) AQUI está o ponto crítico
        df_clean["ARQUIVO_ORIGEM"] = base   # <-- ok! cada df recebe o seu nome

        df_clean.columns = (
            df_clean.columns
            .astype(str)
            .str.replace('"', '', regex=False)
            .str.replace("'", "", regex=False)
            .str.replace(r"\s+", " ", regex=True)
            .str.strip()
            .str.upper()
        )

        # 3) Salva (opcional)
        if output_folder:
            os.makedirs(output_folder, exist_ok=True)
            name, _ = os.path.splitext(base)
            out = os.path.join(output_folder, f"{name}_cleaned.csv")
            df_clean.to_csv(out, index=False, sep=";")

        # 4) Grava no Oracle **arquivo por arquivo**
        if table_name:
            load_to_oracle(df_clean, table_name)

        # 5) Libera memória
        del df_clean

# ============================================================
#  Leitura e limpeza inicial dos leads
# ============================================================
def read_and_clean_leads(file_path, inbox_name) -> pd.DataFrame:
    ext = os.path.splitext(file_path)[1].lower()

    if ext in [".xlsx", ".xls"]:
        df = pd.read_excel(file_path)
    elif ext == ".csv":
        df = pd.read_csv(file_path, sep=",", encoding="utf-8")
    else:
        raise ValueError("Formato inválido. Use CSV ou XLS/XLSX.")

    # Datas
    date_cols = [
        c for c in df.columns
        if any(x in c.lower() for x in ["data", "entrada", "interação", "registro", "nota"])
    ]

    # Numéricos
    numeric_cols = [
        c for c in df.columns
        if df[c].dtype in ["float64", "int64"]
        or c.lower().startswith("valor")
        or "score" in c.lower()
    ]

    # Trata datas
    for col in date_cols:
        df[col] = df[col].apply(parse_date) # type: ignore

    # Numéricos coerção
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Telefones
    if "WhatsApp" in df.columns:
        df["WhatsApp"] = df["WhatsApp"].apply(clean_phone)

    # Coluna obrigatória
    df["inbox_followize"] = inbox_name

    # Remover colunas desnecessárias sem erro
    drop_cols = [
        "Mensagem da Conversão", 
        "Mensagem da anotação", 
        "Mensagem da Finalização",
        "Lead visualizado em",
        "Primeira Interação",
        "Empresa",
        "CNPJ",
        "CPF",
        "Cidade",
        "Estado",
        "CPF do atendente",
        "Equipe",
        "Vendedor",
        "CPF do vendedor",
        "Placa",
        "Chassi",
        "Mídia",
        "Palavra-chave",
        "Formato",
        "FBclid",
        "Gclid",
        "Último agendamento",
        "Motivo do agendamento",
        "Status do agendamento",
        "Última anotação",
        "Valor da proposta",
        "Data da Nota Fiscal",
        "Número da Nota Fiscal",
        "Número da Ficha",
        "Número do Pedido",
        "Valor (R$)",
        "Base legal de processamento",
        "Comunicação para os leads - OPT IN",
        "Base legal de comunicação",
        "Aniversário do cliente",
        "Aniversário de venda"
    ]

    df.drop(columns=[c for c in drop_cols if c in df.columns], inplace=True)

    return df


# ============================================================
#  Carregamento no Oracle
# ============================================================
def load_to_oracle(dataframe: pd.DataFrame, tablename: str):
    try:
        if dataframe.empty:
            print("DataFrame vazio — nada a carregar.")
            return


        dtype_map: dict[str, Any] = {}

        # Strings
        for col in dataframe.select_dtypes(include=["object"]).columns:
            dtype_map[col] = String(255)

        # Números
        for col in dataframe.select_dtypes(include=["float64", "int64"]).columns:
            dtype_map[col] = Float()  # Tipo correto para Oracle

        # Datas
        for col in dataframe.select_dtypes(include=["datetime64[ns]"]).columns:
            dtype_map[col] = DateTime()


        with engine.connect() as conn:
            dataframe.to_sql(
                tablename,
                con=conn,
                index=False,
                if_exists='append',
                dtype=dtype_map, # type: ignore
            )
            print(f"✔ Tabela '{tablename}' carregada com sucesso no Oracle.")

    except SQLAlchemyError as e:
        print("Erro SQLAlchemy:", str(e))


# ============================================================
#  Timestamp auxiliar
# ============================================================
def timestamp_convert(start_date: date = date(1,1,1), start_time: time = time(0,0,0)):
    dt = datetime.combine(start_date, start_time, tzinfo=timezone.utc)
    ts = dt.timestamp()
    print(f"{dt} → timestamp = {int(ts)}")


# ============================================================
#  MAIN
# ============================================================
if __name__ == '__main__':
    csv_path = 'files/online/'
    process_all_leads(input_folder=csv_path, prefix_file='leads_14112025164836', inbox_name='vendas_online', table_name='gb_leads_teste')
    # run_sql()
    # data_ini = date(2020,7,20)
    # data_fim = date(2025,8,20)
    # data1=timestamp_convert(data_ini)
    # data2=timestamp_convert(data_fim)
    