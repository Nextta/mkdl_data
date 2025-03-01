import os
from dotenv import load_dotenv
import requests
import pandas as pd

class mkdl_data:
    def __init__(self):
        load_dotenv()
        self.marketstack_api_key = os.environ.get('marketstack_api_key')

    def get_data_marketstack(self, symbol=None, date_from=None, date_to=None, timeframe=None, sort="ASC"):
        """
        Obtiene datos del mercado desde la API de Marketstack.
        Parámetros:
        symbol (str, opcional): El símbolo de la acción para obtener datos. Por defecto es None.
        date_from (str, opcional): La fecha de inicio para los datos en formato 'YYYY-MM-DD'. Por defecto es None.
        date_to (str, opcional): La fecha de fin para los datos en formato 'YYYY-MM-DD'. Por defecto es None.
        timeframe (str, opcional): El intervalo de tiempo para los datos (por ejemplo, '1min', '5min', '1hour', '1day'). Por defecto es None.
        sort (str, opcional): El orden de los datos ('ASC' para ascendente, 'DESC' para descendente). Por defecto es "ASC".
        Retorna:
        pd.DataFrame: Un DataFrame que contiene los datos del mercado con la fecha como índice.
        Lanza:
        Exception: Si la solicitud a la API de Marketstack falla o devuelve un error.
        """
        if symbol is not None:
            url = f"http://api.marketstack.com/v2/eod?symbols={symbol}&access_key={self.marketstack_api_key}&sort={sort}&limit=1000"

            if date_from is not None:
                url += f"&date_from={date_from}"
            
            if date_to is not None:
                url += f"&date_to={date_to}"
            
            if timeframe is not None:
                url += f"&timeframe={timeframe}"

            response = requests.get(url)

        if response.status_code == 200:
            data = response.json()

            if data['data']:
                df = pd.DataFrame(data['data'])
                df.index = pd.to_datetime(df['date'])
                df = df.drop(columns=['date'])
                return df
        else:
            print("Error: No se optuvo respuesta del servidor")