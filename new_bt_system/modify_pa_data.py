import numpy as np
import pandas as pd

contract_month_ticker_R = {'01': 'F',
                            '02': 'G',
                            '03': 'H',
                            '04': 'J',
                            '05': 'K',
                            '06': 'M',
                            '07': 'N',
                            '08': 'Q',
                            '09': 'U',
                            '10': 'V',
                            '11': 'X',
                            '12': 'Z'}

contract_month_ticker_string = {'Jan': 'F',
                                'Feb': 'G',
                                'Mar': 'H',
                                'Apr': 'J',
                                'May': 'K',
                                'Jun': 'M',
                                'Jul': 'N',
                                'Aug': 'Q',
                                'Sep': 'U',
                                'Oct': 'V',
                                'Nov': 'X',
                                'Dec': 'Z'}


def modify_data(ticker, file_name):
    raw_data = pd.read_csv(r'S:/bjin/FutMinData/Commodities/' + ticker + '/' + file_name + '.csv').rename(columns={
        'datetime': 'time', 'open': 'Open', 'high': 'High', 'low': 'Low', 'last': 'Close', 'volume': 'Volume'})

    raw_data['contract'] = raw_data['symbol'].apply(lambda x: switch_contract2(ticker, x))
    raw_data['product'] = ticker

    raw_data = raw_data.drop(['symbol'], axis=1)

    # save to csv

    raw_data.to_csv(r'S:/bjin/FutMinData/Commodities/' + ticker + '/' + '2020' + '_modified_pa.csv', index=False)


def switch_contract(contract):
    ticker_name = contract[:2]
    ticker_info = contract[2:]

    year = ticker_info[:2]
    month = ticker_info[2:]

    return ticker_name + ' ' + contract_month_ticker_R[month] + '20' + year


def switch_contract2(ticker, contract):
    contract = contract.split('@')[0]

    ticker_name = contract.split('-')[0]
    ticker_info = contract.split('-')[1]

    year = ticker_info[3:]
    month = ticker_info[:3]

    if ticker == 'CU':
        return ticker_name + ' ' + contract_month_ticker_string[month] + '20' + year

    elif ticker == 'AU':
        return ticker_name + 'A' + contract_month_ticker_string[month] + year[-1] + ' Comdty'


if __name__ == '__main__':
    #print(switch_contract2('CU-Feb21@SHFE'))
    modify_data('AU', '2020-pa')