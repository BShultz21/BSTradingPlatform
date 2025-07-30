import asyncio
from typing import Optional
from datetime import datetime, timedelta, timezone
from requests import HTTPError
import Server
import threading
import time
import requests
import base64
from websockets.asyncio.client import connect
import websockets
import json
from pynput.keyboard import Key

class TerminateTaskGroup(Exception):
    pass

class KeyboardHandler(object):
    def __init__(self):
        self.terminate_event = asyncio.Event()

    def on_key_release(self, key):
        if key == Key.f8:
            try:
                self.terminate_event.set()
                return False
            except RuntimeError:
                return False

    async def wait_for_termination(self):
        await self.terminate_event.wait()
        raise TerminateTaskGroup()

class APICredentials(object):
    def __init__(self, api_key:str, secret_key:str, callback_url:str, authorization_url:str, token_url:str, token_file:str) -> None:
        self.appKey = api_key
        self.secretKey = secret_key
        self.callbackUrl = callback_url
        self.encodedCredentials = ''
        self.authUrl = authorization_url + f'?client_id={self.appKey}&redirect_uri={self.callbackUrl}'
        self.tokenUrl = token_url
        self.authCode = ''
        self.accessToken = [None, None]
        self.refreshToken = [None, None]
        self.json = {}
        self.token_file = token_file

    def write_token_data(self) -> None:
        try:
            with open(self.token_file, 'r+') as f:
                data = f.read()
                if data == '':
                    raise ValueError
                data = json.loads(data)

                data["access_token"] = self.accessToken[0]
                data["time_access_token_created"] = self.accessToken[1]
                data["refresh_token"] = self.refreshToken[0]
                data["time_refresh_token_created"] = self.refreshToken[1]

                f.seek(0)
                f.write(json.dumps(data))
                f.truncate()

        except FileNotFoundError:
            print("File does not exist")

        except ValueError:
            print("File is empty")

    def load_token_data(self) -> Optional[dict]:
        """
        Opens a json file and returns the data in dict format
        """
        try:
            with open(self.token_file) as f:
                data = f.read()
                data = json.loads(data)
                return data
        except FileNotFoundError:
            return None

    @staticmethod
    def check_for_valid_refresh_token(token_data: Optional[dict]) -> bool:
        """
        Takes token_data determines if the refresh token is valid
        """
        if token_data and token_data['refresh_token'] and token_data['time_refresh_token_created']:
            current_time = datetime.now()
            token_time = token_data['time_refresh_token_created'][:16]
            token_time = datetime.strptime(token_time, "%Y-%m-%d %H:%M")
            delta = current_time - token_time
            d = timedelta(days=7)
            if delta < d:
                return True
            else:
                return False
        else:
            return False

    @staticmethod
    def check_for_valid_access_token(token_data: Optional[dict]) -> bool:
        if token_data and token_data['access_token'] and token_data["time_access_token_created"]:
            current_time = datetime.now()
            token_time = token_data['time_access_token_created'][:16]
            token_time = datetime.strptime(token_time, "%Y-%m-%d %H:%M")
            delta = current_time - token_time
            d = timedelta(minutes = 30)
            if delta < d:
                return True
            else:
                return False
        else:
            return False

    def get_valid_token(self) -> None:
        """
        If refresh token is valid then assigns class variables
        If token is not valid then starts process of acquiring new tokens
        """
        data = self.load_token_data()
        if self.check_for_valid_refresh_token(data):
            self.refreshToken[0] = data['refresh_token']
            self.refreshToken[1] = data['time_refresh_token_created']
            if self.check_for_valid_access_token(data):
                self.accessToken[0] = data['access_token']
                self.accessToken[1] = data['time_access_token_created']
            else:
                date_time = datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M")
                self.use_refresh_token()
        else:
            self.get_json(self.get_auth_code())
            self.accessToken = self.get_access_token()
            self.refreshToken = self.get_refresh_token()

    def get_auth_code(self) -> str:
        """
        Takes the query returned from authentication server and parses it to return authentication code
        """
        print(self.authUrl)
        threading.Thread(target=Server.run_server, daemon=True).start()
        while self.authCode == '':
            if Server.codes:
                self.authCode = Server.codes[-1]
                return self.authCode
            time.sleep(3)
    def encode_credentials(self) -> str:
        """
        Encodes the client key and secret key provided by api in base64 ascii
        """
        self.encodedCredentials = self.appKey + ':' + self.secretKey
        self.encodedCredentials = base64.b64encode(self.encodedCredentials.encode('ascii')).decode('ascii')
        return self.encodedCredentials

    def get_json(self, auth_code) -> dict:
        """
        Takes authorization code, encoded credentials(client and secret key), and callback url to make post request
        to return json from authentication server
        """
        data = {
            "grant_type": "authorization_code",
            "code": auth_code,
            "redirect_uri": self.callbackUrl
        }
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": f"Basic {self.encode_credentials()}"
        }
        response = requests.post(self.tokenUrl, data=data, headers=headers)
        if response.status_code == 200:
            self.json = response.json()
            return self.json
        else:
            print(f'Error reaching Schwab API, server response code: {response.status_code}')
            raise HTTPError

    def get_access_token(self) -> list:
        """
        Parses a json object to return access token
        """
        self.accessToken[0] = self.json['access_token']
        date_time = datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M")
        self.accessToken[1] = date_time
        return self.accessToken

    def get_refresh_token(self) -> list:
        """
        Parses a json object to return refresh token
        """
        self.refreshToken[0] = self.json['refresh_token']
        date_time = datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M")
        self.refreshToken[1] = date_time
        return self.refreshToken

    def use_refresh_token(self) -> list:
        """
        Uses refresh token to return new access token
        """
        request_data = {
        "grant_type": "refresh_token",
        "refresh_token": self.refreshToken[0],
        "redirect_uri": self.callbackUrl
        }

        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": f"Basic {self.encodedCredentials}"
        }

        response = requests.post(self.tokenUrl, data = request_data, headers = headers)

        try:
            response.raise_for_status()
        except HTTPError:
            message = response.text
            print(f"HTTP Error message: {message}  Status code: {response.status_code}")
            #Need to call authcode
            raise
        data = response.json()
        self.accessToken[0] = data["access_token"]
        date_time = datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M")
        self.accessToken[1] = date_time
        return self.accessToken

class APICalls(object):
    def __init__(self, access_token: str) -> None:
        self.access_token = access_token
        self.auth_header = {
            "Authorization": f"Bearer {self.access_token}"
        }
        self.url = "https://api.schwabapi.com/marketdata/v1/"

    def get_quotes(self, symbols_list:list) -> dict:
        symbols = '%2C'.join(symbols_list)
        url = f"{self.url}quotes?symbols={symbols}"

        response = requests.get(url, headers = self.auth_header)

        if response.status_code == 200:
            return response.json()
        else:
            print(f'Error reaching Schwab API, server response code: {response.status_code}')
            raise HTTPError

class Streamer(object):
    def __init__(self, access_token: str) -> None:
            self.access_token = access_token
            self.url = 'https://api.schwabapi.com/trader/v1/userPreference'
            self.streamer_info = None
            self.websocket = None
            self.message_listener = False
            self.data_queue = None

    def get_streamer_info(self) -> dict:
        headers = {
            "Authorization": f"Bearer {self.access_token}"
        }

        response = requests.get(self.url, headers = headers)
        try:
            if response.status_code == 200:
                streamer_info = response.json()
                streamer_info_list = streamer_info.get('streamerInfo')
                if not streamer_info_list:
                    raise ValueError
                self.streamer_info = streamer_info_list[0]
                return self.streamer_info
            else:
                print(f'Error reaching Schwab API, server response code: {response.status_code}')
                raise HTTPError
        except HTTPError:
            print('test')

    async def start_stream_connection(self) -> None:
        login_request = {
            "requests": [{
                "requestid": "1",
                "service": "ADMIN",
                "command": "LOGIN",
                "SchwabClientCustomerId": self.streamer_info['schwabClientCustomerId'],
                "SchwabClientCorrelId": self.streamer_info['schwabClientCorrelId'],
                "parameters": {
                    "Authorization": self.access_token,
                    "SchwabClientChannel":  self.streamer_info['schwabClientChannel'],
                    "SchwabClientFunctionId": self.streamer_info['schwabClientFunctionId']
                }}
            ]}
        login_data = json.dumps(login_request)

        websocket = await connect(self.streamer_info['streamerSocketUrl'])
        await websocket.send(login_data)
        message = await websocket.recv()
        message = json.loads(message)
        code = message["response"][0]["content"]["code"]
        code_message = message["response"][0]["content"]["msg"]

        if code == 0:
            self.websocket = websocket
        else:
            print(f'Failed to connect websocket to Schwab, code is {code} and message is {code_message}')
            raise ConnectionError

    async def close_stream_connection(self, request_id: int) -> None:
        logout_request = {
            "requests": [{
                "requestid": request_id,
                "service": "ADMIN",
                "command": "LOGOUT",
                "SchwabClientCustomerId": self.streamer_info['schwabClientCustomerId'],
                "SchwabClientCorrelId": self.streamer_info['schwabClientCorrelId'],
                "parameters": {}
            }]}

        logout_request = json.dumps(logout_request)

        await self.websocket.send(logout_request)
        print("Closing Connection")
        await self.websocket.close()

    async def start_message_listener(self) -> None:
        """
        Single coroutine that allows for multiple concurrent websocket streams
        """
        self.message_listener = True

        while self.message_listener:
            try:
                message = await self.websocket.recv()
                await self.handle_message(message)
            except websockets.exceptions.ConnectionClosed:
                print("Connection Closed")
                self.message_listener = False
                break

    async def handle_message(self, message:str) -> list:
        message = json.loads(message)
        if 'data' in message:
            if message['data'][0]['service'] == 'LEVELONE_EQUITIES':
                parsed_message = parse_equities_data(message)
                print(parsed_message)

                if self.data_queue:
                    await self.data_queue.put(parsed_message)

    def set_data_queue(self, queue):
        self.data_queue = queue

    async def request_level_one_equities(self, symbols_list:list, request_id:int) -> None:
        symbols_string = ','.join(symbols_list)
        request_data = {
                 "requests": [{
                   "service": "LEVELONE_EQUITIES",
                   "requestid": request_id,
                   "command": "SUBS",
                   "SchwabClientCustomerId": self.streamer_info['schwabClientCustomerId'],
                   "SchwabClientCorrelId": self.streamer_info['schwabClientCorrelId'],
                   "parameters": {
                    "keys": symbols_string,
                    "fields": "0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51"
                   }}
                 ]}

        request_data = json.dumps(request_data)

        await self.websocket.send(request_data)


    async def request_level_one_options(self, request_id: int) -> None:
        request_data = {
                 "requests": [{
                   "service": "LEVELONE_OPTIONS",
                   "requestid": request_id,
                   "command": "SUBS",
                   "SchwabClientCustomerId": self.streamer_info['schwabClientCustomerId'],
                   "SchwabClientCorrelId": self.streamer_info['schwabClientCorrelId'],
                   "parameters": {
                    "keys": "AAPL  251219C00200000",
                    "fields": "0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55"
                   }}
                 ]}

        request_data = json.dumps(request_data)

        await self.websocket.send(request_data)

    async def request_level_one_futures(self, request_id: int) -> None:
        pass

    async def request_level_one_futures_options(self, request_id: int) -> None:
        pass

    async def request_level_one_futures_forex(self, request_id: int) -> None:
        pass

def parse_equities_data(ticker_data:dict) -> list:
    key_mapping = {
        "key": "Symbol",
        "1": "Bid Price",
        "2": "Ask Price",
        "3": "Last Price",
        "4": "Bid Size",
        "5": "Ask Size",
        "6": "Ask ID",
        "7": "Bid ID",
        "8": "Total Volume",
        "9": "Last Size",
        "10": "High Price",
        "11": "Low Price",
        "12": "Close Price",
        "13": "Exchange ID",
        "14": "Marginable",
        "15": "Description",
        "16": "Last ID",
        "17": "Open Price",
        "18": "Net Change",
        "19": "52 Week High",
        "20": "52 Week Low",
        "21": "PE Ratio",
        "22": "Annual Dividend Amount",
        "23": "Dividend Yield",
        "24": "NAV",
        "25": "Exchange Name",
        "26": "Dividend Date",
        "27": "Regular Market Quote",
        "28": "Regular Market Trade",
        "29": "Regular Market Last Price",
        "30": "Regular Market Last Size",
        "31": "Regular Market Net Change",
        "32": "Security Status",
        "33": "Mark Price",
        "34": "Quote Time in Long",
        "35": "Trade Time in Long",
        "36": "Regular Market Trade Time in Long",
        "37": "Bid Time",
        "38": "Ask Time",
        "39": "Ask MIC ID",
        "40": "Bid MIC ID",
        "41": "Last MIC ID",
        "42": "Net Percent Change",
        "43": "Regular Market Percent Change",
        "44": "Mark Price Net Change",
        "45": "Mark Price Percent Change",
        "46": "Hard to Borrow Quantity",
        "47": "Hard To Borrow Rate",
        "48": "Hard to Borrow",
        "49": "shortable",
        "50": "Post-Market Net Change",
        "51": "Post-Market Percent Change"
    }

    tickers = ticker_data['data'][0]['content']
    revised_tickers = ['Equities']

    for ticker in tickers:
        epoch_timestamp = ticker_data['data'][0]['timestamp'] / 1000
        timestamp = str(datetime.fromtimestamp(epoch_timestamp, tz=timezone.utc))
        timestamp = timestamp[:19]
        revised_ticker_data = {'timestamp': timestamp}
        ticker_copy = {k:v for k,v in ticker.items()}
        for key in ticker.keys():
            if key in key_mapping.keys():
                mapped_value = key_mapping.get(key)
                revised_ticker_data[mapped_value] = ticker_copy.pop(key)

        revised_tickers.append(revised_ticker_data)

    return revised_tickers

def parse_options_data(ticker_data:dict) -> list:
    pass

def parse_futures_data(ticker_data:dict) -> list:
    pass

def parse_futures_options_data(ticker_data:dict) -> list:
    pass

def parse_forex_data(ticker_data:dict) -> list:
    pass




