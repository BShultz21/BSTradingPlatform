from datetime import datetime, timezone
from requests import HTTPError
import requests
from websockets.asyncio.client import connect
import websockets
import json


class Streamer(object):
    def __init__(self, access_token: str) -> None:
            self.access_token = access_token
            self.url = 'https://api.schwabapi.com/trader/v1/userPreference'
            self.streamer_info = None
            self.websocket = None
            self.message_listener = False
            self.data_queue = None

    def get_streamer_info(self) -> dict:
        """
        Makes a get request to receive streamer information that includes SchwabClientCustomerID, SchwabClientCorrelID,
        SchwabClientChannel, and SchwabClientFunctionID
        """
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
        """
        Establishes a stream connection using self.streamer_info. This must be called before any messages
        may be received or sent
        """
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
        """
        Closes a stream connection. The request_id will be the last request sent.
        """
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
        """
        This takes a message provided by the message_listener and determines what service the message is for and then
        adds it into a data queue
        """
        message = json.loads(message)
        if 'data' in message:
            if message['data'][0]['service'] == 'LEVELONE_EQUITIES':
                parsed_message = parse_equities_data(message)
                print(parsed_message)

                if self.data_queue:
                    await self.data_queue.put(parsed_message)
        else:
            print(message)

    def set_data_queue(self, queue):
        """
        Takes in a queue and initializes it to the class
        """
        self.data_queue = queue

    async def request_level_one_equities(self, symbol:str, request_id:int) -> None:
        """
        Takes in a requested symbol and the request_id which should increase as each new request is made
        and prints the requested stock data in a dictionary
        """
        request_data = {
                 "requests": [{
                   "service": "LEVELONE_EQUITIES",
                   "requestid": request_id,
                   "command": "SUBS",
                   "SchwabClientCustomerId": self.streamer_info['schwabClientCustomerId'],
                   "SchwabClientCorrelId": self.streamer_info['schwabClientCorrelId'],
                   "parameters": {
                    "keys": symbol,
                    "fields": "0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51"
                   }}
                 ]}

        request_data = json.dumps(request_data)
        print(request_data)

        await self.websocket.send(request_data)


    async def request_level_one_options(self, request_id: int) -> None:
        """
        Takes in a requested symbol and the request_id which should increase as each new request is made
        and prints the requested option data in a dictionary
        """
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
    """
    This takes in ticker data and maps the keys returned from stream to their respective name
    """
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