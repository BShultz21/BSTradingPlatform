from typing import Optional
from datetime import datetime, timedelta
from requests import HTTPError
import server
import threading
import time
import requests
import base64
import json

class TerminateTaskGroup(Exception):
    pass


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
        """
        Writes access and refresh token data to a .json file with the token information and timestamp
        """
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
        """
        Takes token_data and determines if the access token is greater than 30 minutes old
        """
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
        threading.Thread(target=server.run_server, daemon=True).start()
        while self.authCode == '':
            if server.codes:
                self.authCode = server.codes[-1]
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

