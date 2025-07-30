from random import randint
import time
import pyqtgraph
import qasync
from PyQt6 import QtCore, QtWidgets
import pyqtgraph as pg
from PyQt6.QtCore import QTimer
from PyQt6.QtWidgets import QWidget, QVBoxLayout
from collections import deque
import numpy as np
from TradingPlatform import Streamer, APICredentials, TerminateTaskGroup, KeyboardHandler
from qasync import QEventLoop, QApplication
import asyncio
from dotenv import dotenv_values
from pynput.keyboard import Key, Listener

class MainWindow(QtWidgets.QMainWindow):
    def __init__(self):
        super().__init__()

        self.setWindowTitle("Stock Trading Charts")
        self.setMinimumSize(750, 750)

        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        layout = QVBoxLayout(central_widget)

        self.axis = pg.DateAxisItem(orientation="bottom")
        self.plot_widget = pg.PlotWidget(axisItems={"bottom": self.axis})
        self.plot_widget.setLabel("left", "Price")
        self.plot_widget.setLabel("bottom", "Time")
        self.plot_widget.showGrid(x=True, y=True)
        layout.addWidget(self.plot_widget)

        self.max_data_points = 100
        self.price_list = deque(maxlen=self.max_data_points)
        self.time_list = deque(maxlen=self.max_data_points)

        self.curve = self.plot_widget.plot(self.price_list, self.time_list, pen='r', width=5)

    async def update_plot(self, data):
        if 'Last Price' in data[1]:
            try:
                timestamp = time.time()
                self.time_list.append(timestamp)
                time_array = np.array(list(self.time_list), dtype=np.float64)
                print(f'time is {time}')

                price = float(data[1]['Last Price'])
                print(f'Price is {price}')
                self.price_list.append(price)
                price_array = np.array(list(self.price_list), dtype=np.float64)

                if len(self.price_list) == len(self.time_list):
                    self.curve.setData(time_array, price_array)
                    print("working")

            except KeyError:
                pass

            except TypeError:
                pass



async def stream_data():
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

    async def update_graph():
        while True:
            data = await Stream.data_queue.get()
            await main.update_plot(data)

    try:
        async with asyncio.TaskGroup() as tg:
            task1 = tg.create_task(Stream.start_message_listener())
            task2 = tg.create_task(Stream.request_level_one_equities(['AAPL'], 1))
            task3 = tg.create_task(update_graph())

            termination_task = tg.create_task(keyboard_handler.wait_for_termination())
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

