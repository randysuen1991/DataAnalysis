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
        self.name = name

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
    def __init__(self, start_time, end_time, instrument, name):
        super().__init__(start_time, end_time, instrument, name)
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
        self.last_obs = dict()
        self.new_obs = dict()

    def __call__(self, time, ob, df):
        if time == self.start_time:
            if self.instrument == 'all':
                for key, value in ob.items():
                    self.new_obs[key] = copy.copy(value)
            else:
                instrument_dict = ob[self.instrument]
                self.new_obs[self.instrument] = copy.copy(instrument_dict)

        elif self.end_time > time > self.start_time:
            self._Compute(ob)

        elif time == self.end_time:
            self._Compute(ob)
            self._Record(self.new_obs, df)

    def _Compute(self, ob):
        if self.instrument == 'all':
            for key, value in self.new_obs.items():
                if value != ob[key]:
                    self.last_obs[key] = copy.copy(self.new_obs[key])
                    self.new_obs[key] = copy.copy(ob[key])
        else:
            instrument_dict = ob[self.instrument]
            if instrument_dict != self.new_obs[self.instrument]:
                self.last_obs[self.instrument] = copy.copy(self.new_obs[self.instrument])
                self.new_obs[self.instrument] = copy.copy(ob[self.instrument])

    def _Record(self, ob, df):
        if len(self.last_obs.keys()) == 0 or len(self.new_obs.keys()) == 0:
            return

        if self.instrument == 'all':
            for key, value in self.new_obs.items():
                vol = eval(value['2'])
                if value['1'] >= self.last_obs[key]['9']:
                    df.loc[key, self.name] = -vol
                elif value['1'] <= self.last_obs[key]['4']:
                    df.loc[key, self.name] = vol
                else:
                    df.loc[key, self.name] = 0
        else:
            instrument_dict = self.new_obs[self.instrument]
            vol = eval(instrument_dict['2'])
            if instrument_dict['1'] >= self.last_obs[self.instrument]['9']:
                df.loc[self.instrument, self.name] = -vol
            elif instrument_dict['1'] <= self.last_obs[self.instrument]['4']:
                df.loc[self.instrument, self.name] = vol
            else:
                df.loc[self.instrument, self.name] = 0


class CumulativeTickHandler(DataHandler):
    def __init__(self, start_time, end_time, instrument, name=None):
        super().__init__(start_time, end_time, instrument, name)
        self.obs = dict()
        self.bid_num = dict()
        self.ask_num = dict()
        self.bid_amount = dict()
        self.ask_amount = dict()

    def __call__(self, time, ob, df):
        if time == self.start_time and not self.recorded:
            self.recorded = True
            if self.instrument == 'all':
                for key, value in ob.items():
                    self.obs[key] = copy.copy(value)
            else:
                instrument_dict = ob[self.instrument]
                self.obs[self.instrument] = copy.copy(instrument_dict)
        elif time == self.start_time and self.recorded:
            self._Compute(ob)
        elif time == self.end_time:
            self._Record(None, df)

    def _Compute(self, ob):
        if self.instrument == 'all':
            for key, value in self.obs.items():
                if value != ob[key]:
                    if ob[key]['2'] != self.obs[key]['2']:
                        if eval(ob[key]['1']) >= eval(self.obs[key]['9']):
                            try:
                                self.ask_num[key] += 1
                                self.ask_amount[key] += eval(ob[key]['2'])
                            except KeyError:
                                self.ask_num[key] = 1
                                self.ask_amount[key] = eval(ob[key]['2'])

                        elif eval(ob[key]['1']) <= eval(self.obs[key]['4']):
                            try:
                                self.bid_num[key] += 1
                                self.bid_amount[key] += eval(ob[key]['2'])
                            except KeyError:
                                self.bid_num[key] = 1
                                self.bid_amount[key] = eval(ob[key]['2'])

                    self.obs[key] = copy.copy(ob[key])

        else:
            instrument_dict = ob[self.instrument]
            if instrument_dict != self.obs[self.instrument]:
                if instrument_dict['2'] != self.obs[self.instrument]['2']:
                    if eval(instrument_dict['1']) >= eval(self.obs[self.instrument]['9']):
                        try:
                            self.ask_num[self.instrument] += 1
                            self.ask_amount[self.instrument] += eval(ob[self.instrument]['2'])
                        except KeyError:
                            self.ask_num[self.instrument] = 1
                            self.ask_amount[self.instrument] = eval(ob[self.instrument]['2'])
                    elif eval(instrument_dict['1']) <= eval(self.obs[self.instrument]['4']):
                        try:
                            self.bid_num[self.instrument] += 1
                            self.bid_amount[self.instrument] += eval(ob[self.instrument]['2'])
                        except KeyError:
                            self.bid_num[self.instrument] = 1
                            self.bid_amount[self.instrument] = eval(ob[self.instrument]['2'])
                self.obs[self.instrument] = copy.copy(ob[self.instrument])

    def _Record(self, ob, df):

        if self.instrument == 'all':
            for key, value in self.obs.items():
                try:
                    df.loc[key, 'cubid_time'] = self.bid_num[key]
                except KeyError:
                    df.loc[key, 'cubid_time'] = 0
                try:
                    df.loc[key, 'cuask_time'] = self.ask_num[key]
                except KeyError:
                    df.loc[key, 'cuask_time'] = 0
                try:
                    df.loc[key, 'cubid_vol'] = self.bid_amount[key]
                except KeyError:
                    df.loc[key, 'cubid_vol'] = 0
                try:
                    df.loc[key, 'cuask_vol'] = self.ask_amount[key]
                except KeyError:
                    df.loc[key, 'cuask_vol'] = 0
        else:
            try:
                df.loc[self.instrument, 'cubid_time'] = self.bid_num[self.instrument]
            except KeyError:
                df.loc[self.instrument, 'cubid_time'] = 0
            try:
                df.loc[self.instrument, 'cuask_time'] = self.ask_num[self.instrument]
            except KeyError:
                df.loc[self.instrument, 'cuask_time'] = 0
            try:
                df.loc[self.instrument, 'cubid_vol'] = self.bid_amount[self.instrument]
            except KeyError:
                df.loc[self.instrument, 'cubid_vol'] = 0
            try:
                df.loc[self.instrument, 'cuask_vol'] = self.ask_amount[self.instrument]
            except KeyError:
                df.loc[self.instrument, 'cuask_vol'] = 0
