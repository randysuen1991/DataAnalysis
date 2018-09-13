import pandas as pd
import numpy as np
import copy


# These handlers handle the socket data from MasterLink server.
class DataHandler:
    def __init__(self, start_time, end_time, instrument, name):
        self.instrument = instrument
        self.start_time = start_time
        self.end_time = end_time
        self.recorded = False
        self.done = False
        self.name = name

    def __call__(self, **kwargs):
        time = kwargs.get('time')
        ob = kwargs.get('ob')
        df = kwargs.get('df')
        if time >= self.start_time and not self.recorded:
            self.recorded = True
            self._Compute(ob)

        elif time >= self.end_time and not self.done and self.recorded:
            self.done = True
            self._Record(ob, df)

    def _Compute(self, ob):
        raise NotImplementedError

    def _Record(self, ob, df):
        raise NotImplementedError


class MidPriceHandler(DataHandler):
    def __init__(self, start_time, end_time, instrument, name):
        super().__init__(start_time, end_time, instrument, name)
        self.mid_price_start = dict()

    def _Compute(self, ob):
        if self.instrument == 'all':
            for key, value in ob.items():
                self.mid_price_start[key] = (eval(value['14']) + eval(value['4'])) / 2
        else:
            self.mid_price_start[self.instrument] = (eval(ob[self.instrument]['14']) + eval(ob[self.instrument]['4']))\
                                                    / 2

    def _Record(self, ob, df):
        if self.instrument == 'all':
            for key, value in ob.items():
                mid_price = (eval(value['14']) + eval(value['4'])) / 2
                df.loc[key, self.name] = (mid_price - self.mid_price_start[key]) / self.mid_price_start[key]
        else:
            mid_price = (eval(ob[self.instrument]['14']) + eval(ob[self.instrument]['4'])) / 2
            df.loc[self.instrument, self.name] = (mid_price - self.mid_price_start[self.instrument]) / \
                                                 self.mid_price_start[self.instrument]


class OBPressureHandler(DataHandler):

    bid_index = ['9', '10', '11', '12', '13']
    ask_index = ['19', '20', '21', '22', '23']

    def __init__(self, start_time, end_time, instrument, depth, name):
        super().__init__(start_time, end_time, instrument, name)
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
                df.loc[key, self.name] = bid_amount / ask_amount
        else:
            instrument_dict = ob[self.instrument]
            bid_amount = 0
            for idx in self.bid_index[0:self.depth]:
                bid_amount += eval(instrument_dict[idx])
            ask_amount = 0
            for idx in self.ask_index[0:self.depth]:
                ask_amount += eval(instrument_dict[idx])
            df.loc[self.instrument, self.name] = bid_amount / ask_amount


# This handler would write down the last trade volume.
class LastTickHandler(DataHandler):
    def __init__(self, start_time, end_time, instrument, name):
        super().__init__(start_time, end_time, instrument, name)
        self.orderbook = None
        self.last_info = dict()
        self.second_to_last_info = dict()
        self.last_no_volume = dict()

    def __call__(self, **kwargs):
        time = kwargs.get('time')
        ob = kwargs.get('ob')
        df = kwargs.get('df')
        trade_flags = kwargs.get('trade_flags')
        if self.end_time > time >= self.start_time:
            if self.recorded:
                self._Compute(trade_flags=trade_flags)
            else:
                self.orderbook = ob
                self.recorded = True
                self._Initialize()
                self._Compute(trade_flags=trade_flags)

        elif time >= self.end_time and not self.done:
            self.done = True
            self._Compute(trade_flags=trade_flags)
            self._Record(df)

    def _Initialize(self):
        for key, value in self.orderbook.items():
            self.second_to_last_info[key] = dict()
            self.last_info[key] = dict()
            self.last_info[key]['3'] = self.orderbook[key]['3']
            self.last_info[key]['25'] = self.orderbook[key]['25']
            self.last_no_volume[key] = False

    def _Compute(self, trade_flags, **kwargs):
        if len(trade_flags) > 1:
            self.second_to_last_info[trade_flags[0]] = copy.copy(self.last_info[trade_flags[0]])
            self.last_no_volume[trade_flags[0]] = False
            self.last_info[trade_flags[0]]['3'] = trade_flags[1]
            if len(trade_flags) == 3:
                self.last_info[trade_flags[0]]['25'] = trade_flags[2]
            elif len(trade_flags) == 2:
                self.last_info[trade_flags[0]]['25'] = self.second_to_last_info[trade_flags[0]]['25']
        elif len(trade_flags) == 1:
            self.last_no_volume[trade_flags[0]] = True

    def _Record(self, df):
        for key, value in self.last_info.items():
            if self.last_no_volume[key] is True:
                df.loc[key, self.name] = 0
            else:
                try:
                    vol = eval(self.last_info[key]['3']) - eval(self.second_to_last_info[key]['3'])
                except KeyError:
                    vol = 0
                if self.last_info[key]['25'] == '1':
                    df.loc[key, self.name] = vol
                else:
                    df.loc[key, self.name] = -vol


class CumulativeTickHandler(DataHandler):

    def __init__(self, start_time, end_time, instrument, name=None, **kwargs):
        super().__init__(start_time, end_time, instrument, name)
        self.trade_direction_volume = dict()
        self.orderbook = None

    def __call__(self, **kwargs):
        trade_flags = kwargs.get('trade_flags')
        time = kwargs.get('time')
        ob = kwargs.get('ob')
        df = kwargs.get('df')
        if self.end_time > time >= self.start_time:
            if self.recorded:
                self._Record(df=df, trade_flags=trade_flags)
                self._Compute(trade_flags)
            else:
                self.recorded = True
                self.orderbook = ob
                self._Initialize()
                self._Record(df=df, trade_flags=trade_flags)

        elif time >= self.end_time and not self.done:
            self.done = True
            self._Record(df, trade_flags=trade_flags)
            df.loc[:, 'total_vol'] = df.loc[:, 'cubid_vol'] + df.loc[:, 'cuask_vol']
            df.loc[:, 'vol_diff'] = df.loc[:, 'cuask_vol'] - df.loc[:, 'cubid_vol']
            df.loc[:, 'time_diff'] = df.loc[:, 'cuask_time'] - df.loc[:, 'cubid_time']

    def _Initialize(self):
        if self.instrument == 'all':
            for key, value in self.orderbook.items():
                self.trade_direction_volume[key] = dict()
                self.trade_direction_volume[key]['25'] = value['25']
                self.trade_direction_volume[key]['3'] = value['3']
        else:
            instrument_dict = self.orderbook[self.instrument]
            self.trade_direction_volume[self.instrument] = dict()
            self.trade_direction_volume[self.instrument]['25'] = instrument_dict['25']
            self.trade_direction_volume[self.instrument]['3'] = instrument_dict['3']

    def _Compute(self, trade_flags, **kwargs):
        if len(trade_flags) > 1:
            self.trade_direction_volume[trade_flags[0]]['3'] = trade_flags[1]
            if len(trade_flags) == 3:
                self.trade_direction_volume[trade_flags[0]]['25'] = trade_flags[2]

    def _Record(self, df, **kwargs):
        if len(self.trade_direction_volume.keys()) == 0:
            return
        trade_flags = kwargs.get('trade_flags')
        if len(trade_flags) > 1:
            stock = trade_flags[0]
            vol = eval(trade_flags[1]) - eval(self.trade_direction_volume[stock]['3'])
            if len(trade_flags) == 2:
                if self.trade_direction_volume[stock]['25'] == '2':
                    df.loc[stock, 'cubid_time'] += 1
                    df.loc[stock, 'cubid_vol'] += vol
                elif self.trade_direction_volume[stock]['25'] == '1':
                    df.loc[stock, 'cuask_time'] += 1
                    df.loc[stock, 'cuask_vol'] += vol
            else:
                if trade_flags[2] == '2':
                    df.loc[stock, 'cubid_time'] += 1
                    df.loc[stock, 'cubid_vol'] += vol
                elif trade_flags[2] == '1':
                    df.loc[stock, 'cuask_time'] += 1
                    df.loc[stock, 'cuask_vol'] += vol


class IndexDifferenceHandler(DataHandler):
    def __init__(self, start_time, end_time, instrument, name):
        super().__init__(start_time, end_time, instrument, name)
        self.start_index = None

    def _Compute(self, ob):
        self.start_index = eval(ob[self.instrument]['1'])

    def _Record(self, ob, df):
        df.loc[self.instrument, self.name] = eval(ob[self.instrument]['1']) - self.start_index


class IndexRecordHandler(DataHandler):
    def __init__(self, start_time, end_time, instrument, name):
        super().__init__(start_time, end_time, instrument, name)

    def _Compute(self, ob):
        pass

    def _Record(self, ob, df):
        df.loc[self.instrument, self.name] = eval(ob[self.instrument]['1'])


class HandlerCollection:
    def __init__(self, handler_list):
        self.handelr_list = handler_list

    def __call__(self, **kwargs):
        for handler in self.handelr_list:
            handler(**kwargs)
