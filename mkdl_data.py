import os
from dotenv import load_dotenv
import requests
import pandas as pd
import yfinance as yf

class mkdl_data:
    def __init__(self):
        load_dotenv()
        self.marketstack_api_key = os.environ.get('marketstack_api_key')
        self.eodhd_api_key = os.environ.get('key_EODHD')

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
    
    def get_data_eodhd(self, symbol=None, date_from=None, date_to=None, timeframe="d"):
        """
            Obtiene datos históricos de fin de día para un símbolo dado desde la API de EODHD.
            Parámetros:
            symbol (str): El símbolo de la acción para obtener datos. Por defecto es None.
            date_from (str): La fecha de inicio para obtener datos en formato 'YYYY-MM-DD'. Por defecto es None.
            date_to (str): La fecha de fin para obtener datos en formato 'YYYY-MM-DD'. Por defecto es None.
            timeframe (str): El intervalo de tiempo para los datos. Por defecto es "d" (diario).
            Retorna:
            pd.DataFrame: Un DataFrame que contiene los datos históricos con la fecha como índice.
            Lanza:
            requests.exceptions.RequestException: Si hay un problema con la solicitud de red.
            ValueError: Si el código de estado de la respuesta no es 200.
        """

        if symbol is not None:
            url = f"https://eodhd.com/api/eod/{symbol}?api_token={self.eodhd_api_key}&fmt=json&period={timeframe}"

            if date_from is not None:
                url += f"&from={date_from}"
            
            if date_to is not None:
                url += f"&to={date_to}"

            response = requests.get(url)

            if response.status_code == 200:
                data = response.json()
                df = pd.DataFrame(data)
                df.index = pd.to_datetime(df['date'])
                df = df.drop(columns=['date'])
                return df
    
    def get_data_yfinance(self, symbol=None, date_from=None, date_to=None, timeframe=None):

        if timeframe is not None and symbol is not None:
            if date_from is not None and date_to is not None:
                df = yf.download(symbol, start=date_from, end=date_to, interval=timeframe)
            elif date_from is not None:
                df = yf.download(symbol, start=date_from, interval=timeframe)
            elif date_to is not None:
                df = yf.download(symbol, end=date_to, interval=timeframe)
            else:
                df = yf.download(symbol, interval=timeframe)
        elif symbol is not None:
            if date_from is not None and date_to is not None:
                df = yf.download(symbol, start=date_from, end=date_to)
            elif date_from is not None:
                df = yf.download(symbol, start=date_from)
            elif date_to is not None:
                df = yf.download(symbol, end=date_to)
            else:
                df = yf.download(symbol)

        if df.empty == False:
            df.columns = ['Open', 'High', 'Low', 'Close', 'Volume']
            return df.dropna()
