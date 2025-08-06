import time
import qasync
from PyQt6 import QtWidgets
import pyqtgraph as pg
from PyQt6.QtWidgets import QWidget, QHBoxLayout, QLineEdit, QComboBox, QGridLayout
from collections import deque
import numpy as np
from auth import APICredentials
from data_streamer import Streamer
import asyncio
from dotenv import dotenv_values
from price_history import PriceHistory

class MainWindow(QtWidgets.QMainWindow):
    def __init__(self) -> None:
        super().__init__()

        self.setWindowTitle("Stock Trading Charts")
        self.setMinimumSize(1024, 768)
        self.setContentsMargins(20, 20, 20, 20)

        layout = QHBoxLayout()

        self.axis = pg.DateAxisItem(orientation="bottom")
        self.plot_widget = pg.PlotWidget()
        self.plot_widget.setAxisItems({'bottom': self.axis})
        self.plot_widget.setLabel("left", "Price")
        self.plot_widget.setLabel("bottom", "Time")
        self.plot_widget.showGrid(x=True, y=True)
        layout.addWidget(self.plot_widget)

        chart_options_layout = QGridLayout()

        self.ticker_widget = QLineEdit()
        self.ticker_widget.setMaxLength(5)
        self.ticker_widget.setPlaceholderText("Enter Stock Ticker")
        chart_options_layout.addWidget(self.ticker_widget, 0, 0)

        self.chart_period_widget = QComboBox()
        self.chart_period_widget.addItems(['Day', 'Month', 'Year', 'YTD'])
        chart_options_layout.addWidget(self.chart_period_widget, 1, 0)

        layout.addLayout(chart_options_layout)

        widget = QWidget()
        widget.setLayout(layout)
        self.setCentralWidget(widget)

        self.max_data_points = 1000
        self.price_list = deque(maxlen=self.max_data_points)
        self.time_list = deque(maxlen=self.max_data_points)

        self.curve = self.plot_widget.plot(self.price_list, self.time_list, pen='r', width=5)

    async def update_plot(self, data: dict, streaming:bool) -> None:
        """
        This takes in data and streaming which determines if during market hours and then produces a stock chart in the gui
        Streaming = True means during market hours which streams data. Streaming = False means historical data
        """
        if streaming == True and 'Last Price' in data[1]:
            try:
                timestamp = time.time()
                self.time_list.append(timestamp)
                time_array = np.array(list(self.time_list), dtype=np.float64)

                price = float(data[1]['Last Price'])
                self.price_list.append(price)
                price_array = np.array(list(self.price_list), dtype=np.float64)

                if len(self.price_list) == len(self.time_list):
                    self.curve.setData(time_array, price_array)
            except KeyError:
                pass
            except TypeError:
                pass

        if not streaming:
            for element in data:
                timestamp = element['datetime']
                timestamp = timestamp / 1000
                self.time_list.append(timestamp)

                price = float(element['close'])
                self.price_list.append(price)

            price_array = np.array(list(self.price_list), dtype=np.float64)
            time_array = np.array(list(self.time_list), dtype=np.float64)
            self.curve.setData(time_array, price_array)


    async def wait_for_signal(self, signal):
        """
        Takes in signal, waits until signal has been emitted and the sets future to the desired result
        """
        loop = asyncio.get_running_loop()
        future = loop.create_future()

        def handler(*args):
            future.set_result(args)

        signal.connect(handler)
        result = await future
        return result

    async def requested_ticker(self) -> str:
        await self.wait_for_signal(self.ticker_widget.returnPressed)
        self.plot_widget.setTitle(f'{self.ticker_widget.text().upper()} Stock')
        return self.ticker_widget.text().upper()

def authenticate_user() -> str:
    """
    This authenticates the user through the Schwab API Oauth process and then returns the access token
    """

    config = dotenv_values('../.env')
    Schwab = APICredentials(config['app_key'], config['secret_key'], config['callback_url'],
                            'https://api.schwabapi.com/v1/oauth/authorize',
                            'https://api.schwabapi.com/v1/oauth/token', '../config/tokens/token.json')
    Schwab.encode_credentials()
    Schwab.get_valid_token()
    Schwab.write_token_data()

    return Schwab.accessToken[0]

async def stream_data(access_token:str, ticker:str) -> None:
    """
    This allows streaming of data from Schwab API
    """

    Stream = Streamer(access_token)
    Stream.get_streamer_info()

    Stream.set_data_queue(asyncio.Queue())

    await Stream.start_stream_connection()

    async def update_graph() -> None:
        """
        This grabs data that was set in the queue and then calls to update the plot in the gui
        """
        while True:
            data = await Stream.data_queue.get()
            await main.update_plot(data, True)

    def request_counter(counter: int) -> int:
        counter = counter + 1
        return counter

    task1 = asyncio.create_task(Stream.start_message_listener())
    task2 = asyncio.create_task(Stream.request_level_one_equities(ticker, request_counter(0)))
    task3 = asyncio.create_task(update_graph())

    await task1
    await task2
    await task3


async def main_func():
    access_token = authenticate_user()
    historical = PriceHistory(access_token)

    loop = asyncio.get_running_loop()
    future = loop.create_future()

    ticker = await main.requested_ticker()

    print(historical.get_market_hours('equity'))
    if not historical.get_market_hours('equity')['equity']['EQ']['isOpen']:
        data = historical.get_stock_price_history(ticker)
        data = data['candles']
        await main.update_plot(data, False)
        await future
    else:
        await stream_data(access_token, ticker)


if __name__ == '__main__':
    app = QtWidgets.QApplication([])
    event_loop = qasync.QEventLoop(app)
    asyncio.set_event_loop(event_loop)

    main = MainWindow()
    main.show()

    with event_loop:
        event_loop.run_until_complete(main_func())

