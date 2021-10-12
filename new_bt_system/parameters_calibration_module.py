import numpy as np
import pandas as pd
import datetime as dt
import matplotlib.pyplot as plt


def drift_cal(pair_ticker):
    data_pd = pd.read_csv('ema_data/' + pair_ticker + '_ema_data.csv')

    ema_period = 1200 * 1

    spread_np = np.array(data_pd['spread pct'])
    ema_np = np.array(data_pd['ema'])
    mean_np = np.array(data_pd['spread pct'].rolling(ema_period*2).mean().fillna(method='bfill'))
    std_np = np.array(data_pd['spread pct'].rolling(ema_period*2).std().fillna(method='bfill'))

    edge_np = np.array(data_pd['edge'])
    hv_np = np.array(data_pd['hv'])

    print(std_np)

    plt.subplot(2, 1, 1)
    plt.plot(spread_np)
    plt.plot(ema_np)
    plt.plot(ema_np + 1.5 * std_np)
    plt.plot(ema_np - 1.5 * std_np)

    plt.subplot(2, 1, 2)

    plt.plot(edge_np)

    plt.show()


if __name__ == '__main__':
    pair_ticker = 'SI_AG'
    drift_cal(pair_ticker)