import pandas as pd


class DataHandler:
    def __init__(self, hour, minute, sec):
        pass

    def __call__(self, *args, **kwargs):
        raise NotImplementedError


class MidPriceHandler(DataHandler):

    def __init__(self, hour, minute, sec):
        super().__init__()

    def __call__(self, *args, **kwargs):
        pass


class OBPressureHandler(DataHandler):
    def __init__(self):
        pass

    def __call__(self, *args, **kwargs):
        pass
