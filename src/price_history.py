import requests
from dotenv import dotenv_values
from auth import APICredentials
from requests import HTTPError


class PriceHistory(object):
    def __init__(self, access_token: str) -> None:
        self.access_token = access_token
        self.url = 'https://api.schwabapi.com/marketdata/v1/'
        self.auth_header = {
            "Authorization": f"Bearer {self.access_token}"
        }

    def get_stock_price_history(self, symbol:str) -> dict:
        """
        This return the stock price history for the last year on daily frequency. This also shows extended
        hours data and shows the previous close
        """

        url = f"{self.url}pricehistory?symbol={symbol}&periodType=year&period=1&frequencyType=daily&frequency=1&needExtendedHoursData=true&needPreviousClose=true"

        response = requests.get(url, headers = self.auth_header)

        if response.status_code == 200:
            return response.json()
        else:
            print(f'Error reaching Schwab API, server response code: {response.status_code}')
            raise HTTPError

    def get_market_hours(self, market:str) -> dict:

        market = market.lower()
        url = f'{self.url}markets?markets={market}'

        response = requests.get(url, headers= self.auth_header)

        if response.status_code == 200:
            return response.json()
        else:
            print(f'Error reaching Schwab API, server response code: {response.status_code}')
            raise HTTPError



