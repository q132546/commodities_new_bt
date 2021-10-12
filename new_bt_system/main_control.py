import numpy as np
import pandas as pd
from process_module import *
from backtest_module import *
from backtest_module_param_opt import *
import sys
sys.path.append("..")
from global_var import *
from ema_cal import *


class Main(object):
    def __init__(self):
        self.continuous_ticker_list = ['BO', 'SH', 'SI', 'AG']   # Merge continuous 1 and 2 data if instrument in this list.

    @staticmethod
    def pair_tickers_list():
        #
        return [['HG Z1', 'CU V1'],
                 ['S X1', 'BP V1'],
                 ['BO Z1', 'SH U1'],
                 ['SI Z1', 'AG Z1'],
                 ['GC Z1', 'AU Z1'],
                 ['SM Z1', 'AE F2']]

    def raw_data_merge(self, instrument_list):
        if instrument_list == 'all':
            # instrument_list = all

            tickers_list = []

            for pair in self.pair_tickers_list():
                for instrument in pair:
                    tickers_list.append(instrument.split(' ')[0])

            tickers_list.append('UC')

        else:
            # ex: instrument_list = ['SI', 'AG', 'UC']

            tickers_list = instrument_list

        # ########## merge data ##########

        for ticker in tickers_list:
            print('##### now is ', ticker, ' data merge #####')
            main_contract_process(ticker, 'bt')

            if ticker in self.continuous_ticker_list:
                print('##### now is ', ticker, ' continuous data merge #####')
                continuous_contract_process(ticker)

            else:
                print('##### no continuous data merge #####')

    def backtesting(self, mode):
        bt_pairs_list = []

        for pair in self.pair_tickers_list():
            bt_pairs_list.append(pair[0].split(' ')[0] + '_' + pair[1].split(' ')[0])

        if mode == 'old':
            # fixed parameters skew and pt

            for pair in bt_pairs_list:
                bt = BtFramework(pair, 'bt')
                bt.run_pnl()

        elif mode == 'new':
            # adapted parameters skew and pt
            for pair in bt_pairs_list:
                bt = BtFrameworkParamOpt(pair, 'bt')
                bt.run_pnl()

        else:
            print('### no this mode ###')

    def updated_info(self):
        updated_param = pd.read_csv('updated_info/updated_parameters.csv')

        for i in range(len(updated_param)):
            print(updated_param['Pair'].iloc[i], updated_param['Time'].iloc[i],
                  'skew:', updated_param['skew'].iloc[i], 'pt:', updated_param['pt'].iloc[i])

    def function_control(self):
        # input 'all' or ['SI', 'AG', 'UC'] input the instrument you want to merge
        self.raw_data_merge('all')
        # input 'old' for fixed parameters backtesting or 'new' for adapted parameters backtesting
        self.backtesting('new')
        self.updated_info()


if __name__ == '__main__':
    main_control = Main()
    main_control.function_control()