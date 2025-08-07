import time
import qasync
from PyQt6 import QtWidgets
import qdarktheme
import pyqtgraph as pg
from PyQt6.QtWidgets import QWidget, QHBoxLayout, QLineEdit, QComboBox, QGridLayout, QLabel, QSizePolicy, QPushButton
from PyQt6.QtCore import Qt
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

        qdarktheme.setup_theme()

        self.setWindowTitle("Stock Trading Charts")
        self.setMinimumSize(1024, 768)

        self.layout = QHBoxLayout()

        widget = QWidget()
        widget.setLayout(self.layout)
        self.setCentralWidget(widget)

        self.axis = pg.DateAxisItem(orientation="bottom")
        self.plot_widget = pg.PlotWidget()
        self.plot_widget.setAxisItems({'bottom': self.axis})
        self.plot_widget.setLabel("left", "Price")
        self.plot_widget.setLabel("bottom", "Time")
        self.plot_widget.showGrid(x=True, y=True)
        self.layout.addWidget(self.plot_widget)

        self.max_data_points = 1000
        self.price_list = deque(maxlen=self.max_data_points)
        self.time_list = deque(maxlen=self.max_data_points)

        self.curve = self.plot_widget.plot(self.price_list, self.time_list, pen='r', width=5)

        self.gridLayout = QGridLayout()
        self.layout.addLayout(self.gridLayout)
        self.gridLayout.setSpacing(5)
        self.gridLayout.setContentsMargins(5, 5, 5, 5)

        size_policy = QSizePolicy(QSizePolicy.Policy.Preferred, QSizePolicy.Policy.Fixed)
        size_policy.setHorizontalStretch(0)
        size_policy.setVerticalStretch(0)

        self.frequency_type_label = QLabel("Frequency Type")
        size_policy.setHeightForWidth(self.frequency_type_label.sizePolicy().hasHeightForWidth())
        self.frequency_type_label.setSizePolicy(size_policy)
        self.frequency_type_label.setAlignment(Qt.AlignmentFlag.AlignCenter)

        self.gridLayout.addWidget(self.frequency_type_label, 3, 0, 1, 1)

        self.start_date = QLineEdit()
        self.start_date.setPlaceholderText("Epoch Time (ms)")
        size_policy.setHeightForWidth(self.start_date.sizePolicy().hasHeightForWidth())
        self.start_date.setSizePolicy(size_policy)
        self.start_date.setMaxLength(13)

        self.gridLayout.addWidget(self.start_date, 5, 1, 1, 1)

        self.chart_period_label = QLabel("Chart Period")
        size_policy.setHeightForWidth(self.chart_period_label.sizePolicy().hasHeightForWidth())
        self.chart_period_label.setSizePolicy(size_policy)
        self.chart_period_label.setAlignment(Qt.AlignmentFlag.AlignCenter)

        self.gridLayout.addWidget(self.chart_period_label, 2, 0, 1, 1)

        self.chart_period = QLineEdit()
        self.chart_period.setInputMethodHints(Qt.InputMethodHint.ImhDigitsOnly)
        self.chart_period.setMaxLength(2)

        self.gridLayout.addWidget(self.chart_period, 2, 1, 1, 1)

        self.start_date_label = QLabel("Start Date")
        size_policy.setHeightForWidth(self.start_date_label.sizePolicy().hasHeightForWidth())
        self.start_date_label.setSizePolicy(size_policy)
        self.start_date_label.setAlignment(Qt.AlignmentFlag.AlignCenter)

        self.gridLayout.addWidget(self.start_date_label, 5, 0, 1, 1)

        self.chart_period_type_label = QLabel("Chart Period Type")
        size_policy.setHeightForWidth(self.chart_period_type_label.sizePolicy().hasHeightForWidth())
        self.chart_period_type_label.setSizePolicy(size_policy)
        self.chart_period_type_label.setAlignment(Qt.AlignmentFlag.AlignCenter)

        self.gridLayout.addWidget(self.chart_period_type_label, 1, 0, 1, 1)

        self.frequency_label = QLabel("Frequency")
        size_policy.setHeightForWidth(self.frequency_label.sizePolicy().hasHeightForWidth())
        self.frequency_label.setSizePolicy(size_policy)
        self.frequency_label.setAlignment(Qt.AlignmentFlag.AlignCenter)

        self.gridLayout.addWidget(self.frequency_label, 4, 0, 1, 1)

        self.frequency = QLineEdit()
        self.frequency.setInputMethodHints(Qt.InputMethodHint.ImhDigitsOnly)
        self.frequency.setMaxLength(2)

        self.gridLayout.addWidget(self.frequency, 4, 1, 1, 1)

        self.chart_period_type = QComboBox()
        self.chart_period_type.addItem("Day")
        self.chart_period_type.addItem("Month")
        self.chart_period_type.addItem("Year")
        self.chart_period_type.addItem("YTD")
        self.chart_period_type.setEnabled(True)
        size_policy.setHeightForWidth(self.chart_period_type.sizePolicy().hasHeightForWidth())
        self.chart_period_type.setSizePolicy(size_policy)
        self.chart_period_type.currentIndexChanged.connect(self.change_frequency_type_options)

        self.gridLayout.addWidget(self.chart_period_type, 1, 1, 1, 1)

        self.end_date_label = QLabel("End Date")
        size_policy.setHeightForWidth(self.end_date_label.sizePolicy().hasHeightForWidth())
        self.end_date_label.setSizePolicy(size_policy)
        self.end_date_label.setAlignment(Qt.AlignmentFlag.AlignCenter)

        self.gridLayout.addWidget(self.end_date_label, 6, 0, 1, 1)

        self.ticker = QLineEdit()
        self.ticker.setPlaceholderText("Enter Stock Ticker")
        size_policy.setHeightForWidth(self.ticker.sizePolicy().hasHeightForWidth())
        self.ticker.setSizePolicy(size_policy)
        self.ticker.setMaxLength(5)
        self.ticker.setAlignment(Qt.AlignmentFlag.AlignLeading|Qt.AlignmentFlag.AlignCenter|Qt.AlignmentFlag.AlignVCenter)

        self.gridLayout.addWidget(self.ticker, 0, 0, 1, 2)

        self.end_date = QLineEdit()
        self.end_date.setPlaceholderText("Epoch Time (ms)")
        self.end_date.setMaxLength(13)

        self.gridLayout.addWidget(self.end_date, 6, 1, 1, 1)

        self.frequency_type = QComboBox()
        size_policy.setHeightForWidth(self.frequency_type.sizePolicy().hasHeightForWidth())
        self.frequency_type.setSizePolicy(size_policy)
        self.frequency_type.addItem('Minute')

        self.gridLayout.addWidget(self.frequency_type, 3, 1, 1, 1)

        self.submit_button = QPushButton("Submit")

        self.gridLayout.addWidget(self.submit_button, 7, 0, 1, 2)


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

    async def requested_stock_data(self) -> dict:
        """
        Waits until the submit button is pressed to acquire data from widgets
        """
        await self.wait_for_signal(self.submit_button.clicked)
        data = {'symbol': self.ticker.text().upper(),'periodType' : self.chart_period_type.currentText().lower(),
                'period': self.chart_period.text(), 'frequencyType': self.frequency_type.currentText().lower(),
                'frequency': self.frequency.text(), 'startDate': self.start_date.text(), 'endDate': self.end_date.text()}

        return data

    def change_frequency_type_options(self) -> None:
        """
        Sets the frequency_type combobox options based on the chart_period_type combo box selection
        """
        if self.chart_period_type.currentText().lower() == 'day':
            self.frequency_type.clear()
            self.frequency_type.addItem('Minute')
        elif self.chart_period_type.currentText().lower() == 'month':
            self.frequency_type.clear()
            self.frequency_type.addItems(['Daily', 'Weekly'])
        elif self.chart_period_type.currentText().lower() == 'year':
            self.frequency_type.clear()
            self.frequency_type.addItems(['Daily', 'Weekly', 'Monthly'])
        else:
            self.frequency_type.clear()
            self.frequency_type.addItems(['Daily', 'Weekly'])

    def set_plot_widget_title(self):
        pass

    def validate_stock_data(self, data:dict) -> bool:
        pass

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


    data = await main.requested_stock_data()

    print(historical.get_market_hours('equity'))
    if not historical.get_market_hours('equity')['equity']['EQ']['isOpen']:
        data = historical.get_stock_price_history(data)
        data = data['candles']
        await main.update_plot(data, False)
        await future
    else:
        await stream_data(access_token, data['symbol'])


if __name__ == '__main__':
    app = QtWidgets.QApplication([])
    event_loop = qasync.QEventLoop(app)
    asyncio.set_event_loop(event_loop)

    main = MainWindow()
    main.show()

    with event_loop:
        event_loop.run_until_complete(main_func())

