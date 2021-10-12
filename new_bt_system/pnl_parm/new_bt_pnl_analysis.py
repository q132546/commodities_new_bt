import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import datetime as dt


read_min_data_path = r'S:\Eric\Commodities\backtest_report\min_report\%s.csv'
read_daily_data_path = r'S:\Eric\Commodities\backtest_report\daily_report\%s.csv'


def daily_pnl_analysis(pair_list, last_date, now_date):
    total_daily_pnl = 0
    total_fx_pnl = 0

    for pair in pair_list:
        pnl_pd = pd.read_csv(read_daily_data_path % (pair + '_bt'))
        dates_np = np.array(pnl_pd['Date'])

        # select date

        last_date_index = np.where(dates_np == last_date)[0][0]
        now_date_index = np.where(dates_np == now_date)[0][0]

        last_pnl_pd = pnl_pd.iloc[last_date_index]
        now_pnl_pd = pnl_pd.iloc[now_date_index]

        last_fx_pnl = last_pnl_pd['fx pnl']
        now_fx_pnl = now_pnl_pd['fx pnl']

        last_pnl = last_pnl_pd['total pnl']
        now_pnl = now_pnl_pd['total pnl']

        daily_fx_pnl = now_fx_pnl - last_fx_pnl
        daily_pnl = now_pnl - last_pnl

        total_daily_pnl += daily_pnl
        total_fx_pnl += daily_fx_pnl

        print(pair)
        print('daily pnl', daily_pnl)

    print('###### daily performance ######')
    print('total fx pnl', total_fx_pnl, 'total pnl', total_daily_pnl)


if __name__ == '__main__':
    daily_pnl_analysis(['GC_AU', 'SI_AG', 'S_BP', 'BO_SH', 'SM_AE', 'HG_CU'], '2021-09-15', '2021-09-16')