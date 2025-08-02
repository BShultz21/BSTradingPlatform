import time
import qasync
from PyQt6 import QtWidgets
import pyqtgraph as pg
from PyQt6.QtWidgets import QWidget, QHBoxLayout, QLineEdit
from collections import deque
import numpy as np
from Authentication import APICredentials, TerminateTaskGroup, KeyboardHandler
from DataStreamer import Streamer
import asyncio
from dotenv import dotenv_values
from pynput.keyboard import Listener

class MainWindow(QtWidgets.QMainWindow):
    def __init__(self) -> None:
        super().__init__()

        self.setWindowTitle("Stock Trading Charts")
        self.setMinimumSize(1024, 768)

        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        layout = QHBoxLayout(central_widget)

        self.axis = pg.DateAxisItem(orientation="bottom")
        self.plot_widget = pg.PlotWidget(axisItems={"bottom": self.axis})
        self.plot_widget.setLabel("left", "Price")
        self.plot_widget.setLabel("bottom", "Time")
        self.plot_widget.showGrid(x=True, y=True)
        layout.addWidget(self.plot_widget)

        self.text_widget = QLineEdit()
        self.text_widget.setMaxLength(5)
        self.text_widget.setPlaceholderText("Enter Stock Ticker")
        self.setCentralWidget(central_widget)
        layout.addWidget(self.text_widget)

        self.max_data_points = 1000
        self.price_list = deque(maxlen=self.max_data_points)
        self.time_list = deque(maxlen=self.max_data_points)

        self.curve = self.plot_widget.plot(self.price_list, self.time_list, pen='r', width=5)

    async def update_plot(self, data: dict) -> None:
        """
        This takes in data and then produces a stock chart in the gui
        """
        if 'Last Price' in data[1]:
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
        await self.wait_for_signal(self.text_widget.returnPressed)
        return self.text_widget.text().upper()


async def stream_data() -> None:
    """
    This allows streaming of data from Schwab API
    """
    config = dotenv_values('.env')
    Schwab = APICredentials(config['app_key'], config['secret_key'], config['callback_url'],
                            'https://api.schwabapi.com/v1/oauth/authorize',
                            'https://api.schwabapi.com/v1/oauth/token', 'token.json')
    Schwab.encode_credentials()
    Schwab.get_valid_token()
    Schwab.write_token_data()

    keyboard_handler = KeyboardHandler()

    listener = Listener(on_release=keyboard_handler.on_key_release)
    listener.start()

    Stream = Streamer(Schwab.accessToken[0])
    Stream.get_streamer_info()

    Stream.set_data_queue(asyncio.Queue())

    await Stream.start_stream_connection()

    async def update_graph() -> None:
        """
        This grabs data that was set in the queue and then calls to update the plot in the gui
        """
        while True:
            data = await Stream.data_queue.get()
            await main.update_plot(data)

    def request_counter(counter: int) -> int:
        counter = counter + 1
        return counter

    try:
        ticker = await main.requested_ticker()

        async with asyncio.TaskGroup() as tg:
            task1 = tg.create_task(Stream.start_message_listener())
            task2 = tg.create_task(Stream.request_level_one_equities(ticker, request_counter(0)))
            task3 = tg.create_task(update_graph())

    except* TerminateTaskGroup:
        print("Task Group Terminated")
        await Stream.close_stream_connection(3)

    finally:
        listener.stop()


if __name__ == '__main__':
    app = QtWidgets.QApplication([])
    event_loop = qasync.QEventLoop(app)
    asyncio.set_event_loop(event_loop)

    main = MainWindow()
    main.show()

    with event_loop:
        event_loop.run_until_complete(stream_data())

