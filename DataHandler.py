import pandas as pd
import numpy as np


# These handlers handle the socket data from MasterLink server.
class DataHandler:
    def __init__(self, start_time, end_time, instrument):
        self.instrument = instrument
        self.start_time = start_time
        self.end_time = end_time
        self.recorded = False

    def __call__(self, time, ob, df):
        if time == self.start_time and not self.recorded:
            self.recorded = True
            self._Compute(ob)
        elif time == self.end_time:
            self._Record(ob, df)

    def _Compute(self, ob):
        raise NotImplementedError

    def _Record(self, ob, df):
        raise NotImplementedError


class MidPriceHandler(DataHandler):
    def __init__(self, start_time, end_time, instrument):
        super().__init__(start_time, end_time, instrument)
        self.mid_price_start = dict()

    def _Compute(self, ob):
        if self.instrument == 'all':
            for key, value in ob.items():
                self.mid_price_start[key] = (eval(value['14']) + eval(value['4'])) / 2
        else:
            self.mid_price_start[self.instrument] = (eval(ob[self.instrument]['14']) + eval(ob[self.instrument]['4'])) / 2

    def _Record(self, ob, df):
        if self.instrument == 'all':
            for key, value in ob.items():
                mid_price = (eval(value['14']) + eval(value['4'])) / 2
                df.loc[key, 'mid_price_return'] = (mid_price - self.mid_price_start[key]) / self.mid_price_start[key]
        else:
            mid_price = (eval(ob[self.instrument]['14']) + eval(ob[self.instrument]['4'])) / 2
            df.loc[self.instrument, 'mid_price_return'] = (mid_price - self.mid_price_start[self.instrument]) / \
                                                          self.mid_price_start[self.instrument]


class OBPressureHandler(DataHandler):

    bid_index = ['9', '10', '11', '12', '13']
    ask_index = ['19', '20', '21', '22', '23']

    def __init__(self, start_time, end_time, instrument, depth):
        super().__init__(start_time, end_time, instrument)
        self.depth = depth

    def _Compute(self, ob):
        pass

    def _Record(self, ob, df):
        if self.instrument == 'all':
            for key, value in ob.items():
                bid_amount = 0
                for idx in self.bid_index[0:self.depth]:
                    bid_amount += eval(value[idx])
                ask_amount = 0
                for idx in self.ask_index[0:self.depth]:
                    ask_amount += eval(value[idx])
                df.loc[key, 'obp1'+str(self.depth)] = bid_amount / ask_amount
        else:
            instrument_dict = ob[self.instrument]
            bid_amount = 0
            for idx in self.bid_index[0:self.depth]:
                bid_amount += eval(instrument_dict[idx])
            ask_amount = 0
            for idx in self.ask_index[0:self.depth]:
                ask_amount += eval(instrument_dict[idx])
            df.loc[self.instrument, 'obp1'+str(self.depth)] = bid_amount / ask_amount


# This handler would write down the last trade volume.
class LastTickHandler(DataHandler):
    def __init__(self, start_time, end_time, instrument):
        super().__init__(start_time, end_time, instrument)
        self.last_obs = dict()
        self.new_obs = dict()

    def __call__(self, time, ob, df):
        if time == self.start_time and not self.recorded:
            self._Compute(ob)
        elif time == self.end_time:
            self._Compute(ob)
            self._Record(ob, df)

    def _Compute(self, ob):

        if ob != self.new_ob:
            print('tttttttttttttttttttt')
            self.last_ob = self.new_ob
            self.new_ob = ob

    def _Record(self, ob, df):
        print(self.last_ob)
        print(self.new_ob)
        if self.last_ob is None or self.new_ob is None:
            print(self.last_ob['1303'])
            print(self.new_ob['1330'])
            print('ffffffffffffffffffff')
            return

        if self.instrument == 'all':
            for key, value in self.new_ob.items():
                vol = eval(value['2'])
                if value['9'] == self.last_ob[key]['9'] and value['19'] != self.last_ob[key]['19']:
                    df.loc[key, 'lastvol'] = vol
                elif value['9'] != self.last_ob[key]['9'] and value['19'] == self.last_ob[key]['19']:
                    df.loc[key, 'lastvol'] = -vol
                else:
                    df.loc[key, 'lastvol'] = 0
        else:
            instrument_dict = self.new_ob[self.instrument]
            print(instrument_dict)
            print(self.last_ob['1303'])
            vol = eval(instrument_dict['2'])
            if instrument_dict['9'] == self.last_ob[self.instrument]['9'] and \
                    instrument_dict['19'] != self.last_ob[self.instrument]['19']:
                df.loc[self.instrument, 'lastvol'] = vol
            elif instrument_dict['9'] != self.last_ob[self.instrument]['9'] and \
                    instrument_dict['19'] == self.last_ob[self.instrument]['19']:
                df.loc[self.instrument, 'lastvol'] = -vol
            else:
                df.loc[self.instrument, 'lastvol'] = 0
