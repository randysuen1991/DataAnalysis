import pandas as pd
import numpy as np

class DataHandler:
    def __init__(self, start_time, end_time, instrument):
        self.instrument = instrument
        self.start_time = start_time
        self.end_time = end_time
        self.recorded = False

    def __call__(self, time, df):
        if time == self.start_time and not self.recorded:
            self._Compute()
        elif time == self.end_time:
            pass

    def _Compute(self):
        raise  NotImplementedError


class MidPriceHandler(DataHandler):

    def __init__(self, start_time, end_time, instrument):
        super().__init__(start_time, end_time, instrument)
        self.

    def __call__(self, time, df):
        pass

    def _Compute(self):


class OBPressureHandler(DataHandler):
    def __init__(self, start_time, end_time, instrument):
        super().__init__(start_time, end_time, instrument)

    def __call__(self, time):
        pass
