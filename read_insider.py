from datetime import datetime, date, time, timezone
from typing import Any
from sqlalchemy.types import String, DateTime
from sqlalchemy.exc import SQLAlchemyError
from infra.oracle_database import engine
from sqlalchemy import text
import pandas as pd


def read_csv(file_path):
    columns = [#'a_email',
    'a_name',
    #'a_surname',
    'a_gender',
    #'a_birthday',
    'a_phone_number',
    #'a_unique_user_id',
    #'a_last_purchase_date',
    'a_c_cpf_cnpj',
    'a_c_time_stamp',
    'a_c_cliente_tipo',
    'a_c_cliente_classificacao',
    #'a_c_cliente_cep',
    #'a_c_cliente_uf',
    'a_created_date',
    'a_updated_date',
    'event_name']

    # df_filtrado = df[columns]
    # df_purchase = df_filtrado[df_filtrado['event_name']=='purchase']

    df = pd.read_csv(csv_path)
    df['a_user_action_time_model'] = df['a_user_action_time_model'].str[:255] # Truncates to first 15 characters

    colunas_numericas = [
                "c_os_tipo",
                "c_os_tipo_descricao",
                "c_os_tipo_garantia",
                "c_os_tipo_gera_financeiro",
                "c_os_tipo_revisao"
                ]
    df[colunas_numericas] = df[colunas_numericas].apply(pd.to_numeric, errors="coerce")

    return df
def run_sql(statement):
    with engine.connect() as conn, conn.begin():
        
        #"SELECT * FROM insider_eventos20250820_1"
        
        query = text(statement)
        df = pd.read_sql(query, conn)
        return df
    
def load_to_oracle(dataframe: pd.DataFrame, tablename: str, ):
    try:
      if dataframe.empty:
            print("DataFrame está vazio — nada a carregar.")
            return

      # Cria dicionário de mapeamento para colunas object → String(255)
      dtype_map: dict[str, Any] = {
          col: String(255) for col in dataframe.select_dtypes(include="object").columns
      }

      # Adiciona mapeamento para colunas datetime → DateTime()
      for col in dataframe.select_dtypes(include=["datetime64[ns]"]).columns:
          dtype_map[col] = DateTime()

      with engine.connect() as conn:
          dataframe.to_sql(
                tablename,
                con=conn,
                index=False,
                if_exists='replace',
                dtype=dtype_map # type: ignore
            )
          print(f"Tabela '{tablename}' carregada com sucesso.")
    except SQLAlchemyError as e:
        print(e.__class__.__name__, "-", str(e._message))

def timestamp_convert(start_date: date = date(1,1,1), start_time: time = time(0,0,0)):
    date = datetime.combine(start_date, start_time, tzinfo=timezone.utc)
    ts = date.timestamp()
    return print(f"O datetime {date} convertido para timestamp é {int(ts)}") 


if __name__ == '__main__':
        
    csv_path = 'files/insider_20250820.csv'
    insider = read_csv(csv_path)
    load_to_oracle(dataframe=insider, tablename='insider_eventos20250820_1')
    # run_sql()
    # data_ini = date(2020,7,20)
    # data_fim = date(2025,8,20)
    # data1=timestamp_convert(data_ini)
    # data2=timestamp_convert(data_fim)
    