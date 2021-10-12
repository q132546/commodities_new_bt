import numpy as np
import pandas as pd
import datetime as dt

contract_month_ticker = {'F': '1',
                         'G': '2',
                         'H': '3',
                         'J': '4',
                         'K': '5',
                         'M': '6',
                         'N': '7',
                         'Q': '8',
                         'U': '9',
                         'V': '10',
                         'X': '11',
                         'Z': '12'}


def raw_data_ditch(pair, raw_data):
    if pair in ['CU', 'AG', 'AU']:
        intraday_start_time_list = [dt.datetime.strptime('09:01:00', '%H:%M:%S'), dt.datetime.strptime('10:31:00', '%H:%M:%S'), dt.datetime.strptime('13:31:00', '%H:%M:%S'), dt.datetime.strptime('21:01:00', '%H:%M:%S'), dt.datetime.strptime('00:00:00', '%H:%M:%S')]
        intraday_end_time_list = [dt.datetime.strptime('10:15:00', '%H:%M:%S'), dt.datetime.strptime('11:30:00', '%H:%M:%S'), dt.datetime.strptime('15:00:00', '%H:%M:%S'), dt.datetime.strptime('23:59:00', '%H:%M:%S'), dt.datetime.strptime('01:00:00', '%H:%M:%S')]

        intraday_list = []

        time_dt_pd = raw_data['time'].apply(lambda x: dt.datetime.strptime(x.split(' ')[1], '%H:%M:%S'))

        for time_index in range(len(time_dt_pd)):
            time_dt_temp = time_dt_pd.iloc[time_index]

            for intraday_time_index in range(len(intraday_start_time_list)):
                intraday_start_time = intraday_start_time_list[intraday_time_index]
                intraday_end_time = intraday_end_time_list[intraday_time_index]

                # print(intraday_start_time, intraday_end_time)

                if intraday_start_time <= time_dt_temp <= intraday_end_time:
                    intraday_list.append(time_index)

                else:
                    pass

        intraday_np = np.array(intraday_list)

        raw_data = raw_data.iloc[intraday_np].reset_index(drop=True)

    else:
        pass

    return raw_data


def main_contract_process(pair, mode):
    if mode == 'pa':
        year_list = ['2020']

    else:
        if pair in ['SM', 'AE']:
            year_list = ['2021']

        else:
            year_list = ['2020', '2021']

    raw_data = pd.DataFrame([])

    for year in year_list:
        if mode == 'pa':
            temp_raw_data = pd.read_csv(r'S:/bjin/FutMinData/Commodities/' + pair + '/' + year + '_modified_pa.csv')

        else:
            temp_raw_data = pd.read_csv(r'S:/bjin/FutMinData/Commodities/' + pair + '/' + year + '.csv')

        temp_raw_data = raw_data_ditch(pair, temp_raw_data)

        if pair in ['CU', 'AU', 'AG']:
            print('#### save to raw data ####')

            if mode == 'pa':
                temp_raw_data.to_csv(r'S:/bjin/FutMinData/Commodities/' + pair + '/' + year + '_modified_pa.csv', index=False)
            else:
                temp_raw_data.to_csv(r'S:/bjin/FutMinData/Commodities/' + pair + '/' + year + '.csv', index=False)

        else:
            pass

        if len(raw_data) == 0:
            raw_data = temp_raw_data

        else:
            raw_data = pd.concat([raw_data, temp_raw_data], axis=0, sort=True)

    raw_data_contracts_np = np.array(raw_data['contract'])

    #raw_data_times_np = np.array(raw_data['time'], dtype=np.datetime64)

    # merge data

    contract_table = pd.read_csv('contract/' + pair + '_contract.csv')
    contract_table_date = np.array(contract_table['Date'])
    contract_table_contract = np.array(contract_table['Contract'])

    merge_data_pd = pd.DataFrame([])

    # find contract

    for i in range(len(contract_table_contract)):
        if pair in ['CL', 'CO', 'S', 'BP', 'BO', 'SH', 'SCP']:
            temp_contract = contract_table_contract[i] + ' Comdty'

        elif pair in ['FFB']:
            temp_contract = contract_table_contract[i] + ' Index'

        elif pair in ['XU']:
            temp_contract = 'XU ' + contract_table_contract[i][-2] + '202' + contract_table_contract[i][-1]

        elif pair in ['UC']:
            temp_contract = 'UC ' + contract_table_contract[i][-2] + '202' + contract_table_contract[i][-1]

        elif pair in ['CU', 'AG', 'AU', 'GC', 'SI', 'HG', 'SM', 'AE']:
            temp_contract = contract_table_contract[i]

        else:
            temp_contract = 'None'

        start = np.datetime64(contract_table_date[i])

        if i != len(contract_table_contract) - 1:
            end = np.datetime64(contract_table_date[i + 1])

        else:
            end = np.datetime64('now') + np.timedelta64(1,'D')

        print(start, end)

        if temp_contract in raw_data_contracts_np:
            # contract selection

            index = np.where(raw_data_contracts_np == temp_contract)[0]

            temp_contract_raw_data = raw_data.iloc[index].drop_duplicates(subset=['time'])

            try:
                temp_contract_times_np = np.array(temp_contract_raw_data['time'], dtype=np.datetime64)

                # time selection

                start_index = np.where(temp_contract_times_np >= start)[0][0]
                end_index = np.where(temp_contract_times_np < end)[0][-1]

                if len(merge_data_pd) == 0:
                    merge_data_pd = temp_contract_raw_data.iloc[start_index: end_index]

                else:
                    merge_data_pd = pd.concat([merge_data_pd, temp_contract_raw_data.iloc[start_index: end_index]],
                                              axis=0, sort=False)

            except ValueError:
                pass

        else:
            print('no this contract')

    merge_data_pd = merge_data_pd.rename(columns={'contract': 'Contract'}).reset_index(inplace=False).drop(['index'], axis=1).drop_duplicates(subset=['time'])

    # save merge data

    print('#### save to data ####')

    if mode == 'pa':
        merge_data_pd.to_csv(r'S:/bjin/FutMinData/Commodities/' + pair + '/' + pair + '-pa.csv', index=False)

    else:
        merge_data_pd.to_csv(r'S:/bjin/FutMinData/Commodities/' + pair + '/' + pair + '.csv', index=False)


def continuous_contract_process(pair):
    if pair in ['SM', 'AE']:
        year_list = ['2021']

    else:
        year_list = ['2020', '2021']

    raw_data = pd.DataFrame([])

    for year in year_list:
        temp_raw_data = pd.read_csv(r'S:/bjin/FutMinData/Commodities/' + pair + '/' + year + '.csv')
        temp_raw_data = raw_data_ditch(pair, temp_raw_data)

        if pair in ['CU', 'AU', 'AG']:
            print('#### save to raw data ####')

            temp_raw_data.to_csv(r'S:/bjin/FutMinData/Commodities/' + pair + '/' + year + '.csv', index=False)

        else:
            pass

        if len(raw_data) == 0:
            raw_data = temp_raw_data

        else:
            raw_data = pd.concat([raw_data, temp_raw_data], axis=0, sort=True)

    raw_data_contracts_np = np.array(raw_data['contract'])

    #raw_data_times_np = np.array(raw_data['time'], dtype=np.datetime64)

    # merge data

    contract_table = pd.read_csv('contract/' + pair + '_continuous_contract.csv')

    contract_table_date = np.array(contract_table['Date'])
    contract1_table_contract = np.array(contract_table['Contract1'])
    contract2_table_contract = np.array(contract_table['Contract2'])

    merge_data1_pd = pd.DataFrame([])
    merge_data2_pd = pd.DataFrame([])

    # find contract

    for i in range(len(contract_table_date)):
        if pair in ['CL', 'CO', 'S', 'BP', 'BO', 'SH', 'SCP']:
            temp_contract1 = contract1_table_contract[i] + ' Comdty'
            temp_contract2 = contract2_table_contract[i] + ' Comdty'

        elif pair in ['FFB']:
            temp_contract1 = contract1_table_contract[i] + ' Index'
            temp_contract2 = contract2_table_contract[i] + ' Index'

        elif pair in ['XU']:
            temp_contract1 = 'XU ' + contract1_table_contract[i][-2] + '202' + contract1_table_contract[i][-1]
            temp_contract2 = 'XU ' + contract2_table_contract[i][-2] + '202' + contract2_table_contract[i][-1]

        elif pair in ['UC']:
            temp_contract1 = 'UC ' + contract1_table_contract[i][-2] + '202' + contract1_table_contract[i][-1]
            temp_contract2 = 'UC ' + contract2_table_contract[i][-2] + '202' + contract2_table_contract[i][-1]

        elif pair in ['CU', 'AG', 'AU', 'GC', 'SI', 'HG', 'SM', 'AE']:
            temp_contract1 = contract1_table_contract[i]
            temp_contract2 = contract2_table_contract[i]

        else:
            temp_contract1 = 'None'
            temp_contract2 = 'None'

        start = np.datetime64(contract_table_date[i])

        if i != len(contract_table_date) - 1:
            end = np.datetime64(contract_table_date[i + 1])

        else:
            end = np.datetime64('now') + np.timedelta64(1,'D')

        if temp_contract1 in raw_data_contracts_np:
            # contract selection

            index1 = np.where(raw_data_contracts_np == temp_contract1)[0]
            index2 = np.where(raw_data_contracts_np == temp_contract2)[0]

            temp_contract1_raw_data = raw_data.iloc[index1].drop_duplicates(subset=['time'])
            temp_contract2_raw_data = raw_data.iloc[index2].drop_duplicates(subset=['time'])

            try:
                temp_contract1_times_np = np.array(temp_contract1_raw_data['time'], dtype=np.datetime64)
                temp_contract2_times_np = np.array(temp_contract2_raw_data['time'], dtype=np.datetime64)

                # time selection

                start_index1 = np.where(temp_contract1_times_np >= start)[0][0]
                end_index1 = np.where(temp_contract1_times_np < end)[0][-1]

                start_index2 = np.where(temp_contract2_times_np >= start)[0][0]
                end_index2 = np.where(temp_contract2_times_np < end)[0][-1]

                # continuous contract 1

                if len(merge_data1_pd) == 0:
                    merge_data1_pd = temp_contract1_raw_data.iloc[start_index1: end_index1]

                else:
                    merge_data1_pd = pd.concat([merge_data1_pd, temp_contract1_raw_data.iloc[start_index1: end_index1]], axis=0, sort=False)

                # continuous contract 2

                if len(merge_data2_pd) == 0:
                    merge_data2_pd = temp_contract2_raw_data.iloc[start_index2: end_index2]

                else:
                    merge_data2_pd = pd.concat([merge_data2_pd, temp_contract2_raw_data.iloc[start_index2: end_index2]], axis=0, sort=False)

            except ValueError:
                pass

        else:
            print('no this contract')

    merge_data1_pd = merge_data1_pd.rename(columns={'contract': 'Contract1'}).reset_index(inplace=False).drop(['index'], axis=1).drop_duplicates(subset=['time'])
    merge_data2_pd = merge_data2_pd.rename(columns={'contract': 'Contract2'}).reset_index(inplace=False).drop(['index'], axis=1).drop_duplicates(subset=['time'])

    # save merge data

    print('#### save to continuous data ####')

    merge_data1_pd.to_csv(r'S:/bjin/FutMinData/Commodities/' + pair + '/' + pair + '1.csv', index=False)
    merge_data2_pd.to_csv(r'S:/bjin/FutMinData/Commodities/' + pair + '/' + pair + '2.csv', index=False)


if __name__ == '__main__':
    bt_ticker_list = ['HG', 'CU', 'SI', 'AG', 'GC', 'AU', 'UC', 'S', 'BP', 'BO', 'SH', 'XU', 'FFB', 'AE', 'SM']
    bt_ticker_list = ['UC']
    bt_continuous_ticker_list = ['BO', 'SH', 'SI', 'AG']
    pa_ticker_list = ['CU', 'AG', 'AU']

    for ticker in bt_ticker_list:
        print('##### now is ', ticker, ' #####')
        main_contract_process(ticker, 'bt')

        if ticker in bt_continuous_ticker_list:
            continuous_contract_process(ticker)