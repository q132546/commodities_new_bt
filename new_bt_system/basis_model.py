import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


class BasisFrameWork():
    def __init__(self, pair_ticker):
        self.pair_ticker = pair_ticker
        self.ticker1 = pair_ticker.split('_')[0]
        self.ticker2 = pair_ticker.split('_')[1]

        self.merge_data_pd = self.read_data()

    def read_data(self):
        ticker1_contract1_data = pd.read_csv(r'S:/bjin/FutMinData/Commodities/' + self.ticker1 + '/' + self.ticker1 + '1.csv')[['time', 'Close', 'Contract1']]\
            .rename(columns={'Close': self.ticker1 + '_Close1', 'Contract1': self.ticker1 + '_Contract1'})
        ticker1_contract2_data = pd.read_csv(r'S:/bjin/FutMinData/Commodities/' + self.ticker1 + '/' + self.ticker1 + '2.csv')[['time', 'Close', 'Contract2']]\
            .rename(columns={'Close': self.ticker1 + '_Close2', 'Contract2': self.ticker1 + '_Contract2'})

        ticker2_contract1_data = pd.read_csv(r'S:/bjin/FutMinData/Commodities/' + self.ticker2 + '/' + self.ticker2 + '1.csv')[['time', 'Close', 'Contract1']]\
            .rename(columns={'Close': self.ticker2 + '_Close1', 'Contract1': self.ticker2 + '_Contract1'})
        ticker2_contract2_data = pd.read_csv(r'S:/bjin/FutMinData/Commodities/' + self.ticker2 + '/' + self.ticker2 + '2.csv')[['time', 'Close', 'Contract2']]\
            .rename(columns={'Close': self.ticker2 + '_Close2', 'Contract2': self.ticker2 + '_Contract2'})

        ticker1_merge_data_pd = pd.merge(ticker1_contract1_data, ticker1_contract2_data, on='time')
        ticker2_merge_data_pd = pd.merge(ticker2_contract1_data, ticker2_contract2_data, on='time')

        pair_merge_data_pd = pd.merge(ticker1_merge_data_pd, ticker2_merge_data_pd, on='time')

        # cal basis spread and spread pct

        pair_merge_data_pd[self.ticker1 + '_basis_spread'] = pair_merge_data_pd[self.ticker1 + '_Close1'] - pair_merge_data_pd[self.ticker1 + '_Close2']
        pair_merge_data_pd[self.ticker1 + '_basis_spread_pct'] = (pair_merge_data_pd[self.ticker1 + '_Close1'] - pair_merge_data_pd[self.ticker1 + '_Close2']) / pair_merge_data_pd[self.ticker1 + '_Close2']

        pair_merge_data_pd[self.ticker2 + '_basis_spread'] = pair_merge_data_pd[self.ticker2 + '_Close1'] - pair_merge_data_pd[self.ticker2 + '_Close2']
        pair_merge_data_pd[self.ticker2 + '_basis_spread_pct'] = (pair_merge_data_pd[self.ticker2 + '_Close1'] - pair_merge_data_pd[self.ticker2 + '_Close2']) / pair_merge_data_pd[self.ticker2 + '_Close2']

        return pair_merge_data_pd

    def analysis(self):
        ticker1_spread_pct = np.array(self.merge_data_pd[self.ticker1 + '_basis_spread_pct'])
        ticker2_spread_pct = np.array(self.merge_data_pd[self.ticker2 + '_basis_spread_pct'])

        plt.plot(ticker1_spread_pct, 'r')
        plt.plot(ticker2_spread_pct, 'g')

        plt.show()

if __name__ == '__main__':
    basis_framework = BasisFrameWork('S_BP')
    basis_framework.analysis()