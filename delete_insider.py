from dotenv import load_dotenv
from read_insider import run_sql
from loguru import logger
import requests
from tqdm import tqdm
import json
import time
import os
import sys

# Remove saída padrão no console
logger.remove()

# Adiciona apenas saída para arquivo
logger.add("logs/file_{time}.log", rotation="10 MB", encoding="utf-8", enqueue=True)

load_dotenv()

URL = "https://unification.useinsider.com/api/user/v1/delete" # os.getenv('URL_INSIDER')
PARTNER_NAME = os.getenv('PARTNER_NAME')
TOKEN = os.getenv('TOKEN')

headers = {
  'X-PARTNER-NAME': PARTNER_NAME,
  'X-REQUEST-TOKEN': TOKEN,
  'Content-Type': 'application/json'
}

@logger.catch
def delete_insder_users(headers, url):

    query = "SELECT * FROM cpfs_duplicados_temp"

    # dataframe = run_sql(query)
    dataframe = ["32591960801", "00465423531","15398780824","43709108896"]

    dataframe_size = len(dataframe)
    cadence = (dataframe_size/900)/60

    counter_success = 0
    counter_error = 0

    start = time.time()
    try:
      for cliente in tqdm(dataframe, desc="Deleting", unit="row", file=sys.stderr, leave=True):
      # for cliente in dataframe: #['cod_cliente']:
          payload = json.dumps({
            "identifiers": {
              "custom": {
                  "cpf_cnpj": f"{cliente}"
              }
            }
          })

          time.sleep(cadence)
          response = requests.request("POST", url=URL, headers=headers, data=payload, timeout=15)

          # Verifica se a requisição foi bem-sucedida
          if response.status_code in [200, 202, 204]:
              logger.info(f"[SUCESSO] Usuario '{cliente}' excluído com sucesso. Status: {response.status_code}")
              counter_success+=1
          else:
              logger.warning(f"[ERRO] Falha ao excluir perfil '{cliente}'. Status: {response.status_code}, Resposta: {response.text}")
              counter_error+=1
    except requests.exceptions.RequestException as e:
            logger.error(f"[ERRO CRÍTICO] Ocorreu um erro de conexão ao tentar excluir '{cliente}': {e}")
            return False
        
    
    end = time.time()
    tempo = end-start

    print(f"Processo concluído. \n \
                Total de registros deletados: {counter_success} \n \
                Total de registros com falha ou não localizados: {counter_error} \n \
                Tempo decorrido: {tempo:.2f} segundos")

if __name__ == '__main__':
    


    delete_insder_users(headers, URL)