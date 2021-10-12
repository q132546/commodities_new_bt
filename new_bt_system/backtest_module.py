import numpy as np
import pandas as pd
import datetime as dt
import matplotlib.pyplot as plt
from calculation_module import *

read_merged_data_path = r'S:\bjin\FutMinData\Commodities\%s\%s.csv'
save_min_data_path = r'S:\Eric\Commodities\backtest_report\min_report\%s.csv'
save_daily_data_path = r'S:\Eric\Commodities\backtest_report\daily_report\%s.csv'


class BtFramework(object):
    def __init__(self, pair_ticker, mode):
        self.pair_ticker = pair_ticker
        self.mode = mode

        self.lead_ticker = pair_ticker.split('_')[0]
        self.dom_ticker = pair_ticker.split('_')[1]
        self.fx = 'UC'

        self.bt_start_time = '2021-01-01 00:00:00'
        self.bt_end_time = '2021-12-31 09:00:00'

        self.data_pd = pd.DataFrame()

        self.parameter = pd.read_csv('parameter.csv', index_col='pair')
        self.universe = pd.read_csv('universe.csv', index_col='Product')

        self.lead_mltp = self.universe.loc[self.lead_ticker]['Multiplier']
        self.dom_mltp = self.universe.loc[self.dom_ticker]['Multiplier']

    def run_pnl(self):
        self.prepare()
        self.parameters_cal()
        self.backtesting_cal()

    def prepare(self):
        # -------- merge data ---------
        if self.fx == 'UC':
            lead_data_pd = pd.read_csv(read_merged_data_path % (self.lead_ticker, self.lead_ticker)).\
                rename(columns={"Close": "lead_close", "Open": "lead_open",  "High": "lead_high", "Low": "lead_low", "Contract": "lead_contract"})
            if self.mode == 'pa':
                dom_data_pd = pd.read_csv(read_merged_data_path % (self.dom_ticker, self.dom_ticker + '-pa')).\
                    rename(columns={"Close": "dom_close", "Open": "dom_open", "High": "dom_high", "Low": "dom_low", "Contract": "dom_contract"})

            else:
                dom_data_pd = pd.read_csv(read_merged_data_path % (self.dom_ticker, self.dom_ticker)). \
                    rename(columns={"Close": "dom_close", "Open": "dom_open", "High": "dom_high", "Low": "dom_low", "Contract": "dom_contract"})

            fx_data_pd = pd.read_csv(read_merged_data_path % (self.fx, self.fx)).\
                rename(columns={"Close": "fx_close", "Contract": "fx_contract"})

            self.data_pd = pd.merge(lead_data_pd, dom_data_pd, on='time')
            self.data_pd = pd.merge(self.data_pd, fx_data_pd, on='time').dropna()

            # time selection

            date_dt_pd = self.data_pd['time'].apply(lambda x: dt.datetime.strptime(x, '%Y-%m-%d %H:%M:%S'))

            # bt period selection

            start = dt.datetime.strptime(self.bt_start_time, '%Y-%m-%d %H:%M:%S')
            end = dt.datetime.strptime(self.bt_end_time, '%Y-%m-%d %H:%M:%S')

            start_index = np.where(date_dt_pd >= start)[0][0]
            end_index = np.where(date_dt_pd <= end)[0][-1]

            self.data_pd = self.data_pd.iloc[start_index:end_index]

            # intraday time selection

            intraday_start_time_list = [dt.datetime.strptime('09:01:00', '%H:%M:%S'), dt.datetime.strptime('10:31:00', '%H:%M:%S'), dt.datetime.strptime('13:31:00', '%H:%M:%S'), dt.datetime.strptime('21:01:00', '%H:%M:%S'), dt.datetime.strptime('00:00:00', '%H:%M:%S')]
            intraday_end_time_list = [dt.datetime.strptime('10:15:00', '%H:%M:%S'), dt.datetime.strptime('11:30:00', '%H:%M:%S'), dt.datetime.strptime('15:00:00', '%H:%M:%S'), dt.datetime.strptime('23:59:00', '%H:%M:%S'), dt.datetime.strptime('01:00:00', '%H:%M:%S')]

            intraday_list = []

            time_dt_pd = self.data_pd['time'].apply(lambda x: dt.datetime.strptime(x.split(' ')[1], '%H:%M:%S'))

            for time_index in range(len(time_dt_pd)):
                time_dt_temp = time_dt_pd.iloc[time_index]

                for intraday_time_index in range(len(intraday_start_time_list)):
                    intraday_start_time = intraday_start_time_list[intraday_time_index]
                    intraday_end_time = intraday_end_time_list[intraday_time_index]

                    #print(intraday_start_time, intraday_end_time)

                    if intraday_start_time <= time_dt_temp <= intraday_end_time:
                        intraday_list.append(time_index)

                    else:
                        pass

            intraday_np = np.array(intraday_list)
            self.data_pd = self.data_pd.iloc[intraday_np].reset_index(drop=True)

        else:
            pass

    def parameters_cal(self):
        # calculate parameters

        parameters_cal_object = ParametersCalculation(self.pair_ticker, self.data_pd)
        parameters_cal_object.ema_cal()
        parameters_cal_object.pt_skew_cal('const')
        parameters_cal_object.param_cal('symmetric')

        self.data_pd = parameters_cal_object.data_pd

    def backtesting_cal(self):
        # time selection

        # start = dt.datetime.strptime(self.bt_start_time, '%Y-%m-%d %H:%M:%S')
        # date_dt_pd = self.data_pd['time'].apply(lambda x: dt.datetime.strptime(x, '%Y-%m-%d %H:%M:%S'))
        # start_index = np.where(date_dt_pd >= start)[0][0]

        data_pd = self.data_pd

        # bt main

        # ----- data setting -----

        dates_np = np.array(data_pd['time'].apply(lambda x: x.split(' ')[0]))
        time_np = np.array(data_pd['time'].apply(lambda x: x.split(' ')[1]))

        lead_contract_np = np.array(data_pd['lead_contract'])
        dom_contract_np = np.array(data_pd['dom_contract'])
        fx_contract_np = np.array(data_pd['fx_contract'])

        lead_close_np = np.array(data_pd['lead_close'])
        dom_close_np = np.array(data_pd['dom_close'])
        fx_close_np = np.array(data_pd['fx_close'])

        edge_close_np = np.array(data_pd['edge'])

        p1_np = np.array(data_pd['p1'])
        pt_np = np.array(data_pd['pt'])
        skew_np = np.array(data_pd['skew'])
        notional_np = np.array(data_pd['notional'])
        ema_np = np.array(data_pd['ema'])
        spread_pct_np = np.array(data_pd['spread pct'])

        # ----- start bt -----

        quantity = self.parameter.loc[self.pair_ticker]['qty']

        lead_fee = self.universe.loc[self.lead_ticker]['commission']
        dom_fee = self.universe.loc[self.dom_ticker]['commission']
        fx_fee = self.universe.loc[self.fx]['commission']

        lead_pnl_event = {'pnl': 0, 'pnl list': [], 'position': 0, 'price': 0, 'contract': lead_contract_np[0], 'lead_limit_check': 0}
        dom_pnl_event = {'pnl': 0, 'pnl list': [], 'position': 0, 'price': 0, 'contract': dom_contract_np[0], 'dom_limit_check': 0}
        fx_event = {'pnl': 0, 'pnl list': [], 'position': 0, 'price': fx_close_np[0], 'contract': fx_contract_np[0]}

        parameters_event = {'p1': p1_np[0], 'date': dates_np[0], 'last_skew': skew_np[0], 'last_edge': edge_close_np[0], 'p1_signal': 0, 'p2_signal': 0, 'switch_bars': 0, 'deal_qty_limit': 3}

        daily_list = []
        min_list = []

        for i in range(len(dates_np)):
            dates = dates_np[i]
            time = time_np[i]

            lead_close = lead_close_np[i]
            dom_close = dom_close_np[i]
            fx_close = fx_close_np[i]

            # price limit check

            if lead_close == lead_pnl_event['price']:
                lead_pnl_event['lead_limit_check'] += 1

            else:
                lead_pnl_event['lead_limit_check'] = 0

            if dom_close == dom_pnl_event['price']:
                dom_pnl_event['dom_limit_check'] += 1

            else:
                dom_pnl_event['dom_limit_check'] = 0

            # ============= price limit check end ================

            edge = edge_close_np[i]
            spread_pct = spread_pct_np[i]

            lead_contract = lead_contract_np[i]
            dom_contract = dom_contract_np[i]
            fx_contract = fx_contract_np[i]

            skew = skew_np[i]
            max_notion = notional_np[i]
            pt = pt_np[i]

            p1 = parameters_event['p1']
            p2 = p1 - pt

            trading_button = 0   # monitor if trade then set deal price as next bar open
            deal_qty = 0

            print(dates, time, ema_np[i], spread_pct_np[i])
            print(lead_pnl_event['position'], dom_pnl_event['position'], edge, 'p1', p1, 'p2', p2)
            print(lead_pnl_event['pnl'], dom_pnl_event['pnl'])
            print('#################')
            print(edge, p1 + 0.03, p2 - 0.03, lead_pnl_event['lead_limit_check'], dom_pnl_event['dom_limit_check'], abs(edge - parameters_event['last_edge']), parameters_event['switch_bars'])

            # ============= parameters calibration =============

            # -------- if contract switch --------------

            if lead_contract != lead_pnl_event['contract'] or dom_contract != dom_pnl_event['contract'] or fx_contract != fx_event['contract']:
                lead_pnl_event['position'] = 0
                dom_pnl_event['position'] = 0
                fx_event['position'] = 0

                # parameters clean

                parameters_event['p1'] = p1_np[i]
                p1 = parameters_event['p1']
                parameters_event['switch_bars'] = 0

                print(dates, time, 'switch')

            else:
                parameters_event['switch_bars'] += 1

            # -------- if skew adjust ---------

            if skew != parameters_event['last_skew']:
                lead_pnl_event['position'] = 0
                dom_pnl_event['position'] = 0
                fx_event['position'] = 0

                # parameters clean

                parameters_event['p1'] = p1_np[i]
                p1 = parameters_event['p1']

                print(dates, time, 'switch')

            else:
                pass

            # -------- calculate this tick pnl ---------

            lead_pnl_event['pnl'] += (lead_close - lead_pnl_event['price']) * lead_pnl_event['position'] * self.lead_mltp
            lead_pnl_event['pnl list'].append(lead_pnl_event['pnl'])

            dom_pnl_event['pnl'] += (dom_close / fx_close - dom_pnl_event['price'] / fx_event['price']) * dom_pnl_event['position'] * self.dom_mltp
            dom_pnl_event['pnl list'].append(dom_pnl_event['pnl'])

            fx_event['pnl'] += ((fx_close - fx_event['price']) / fx_close) * 100000 * fx_event['position']
            fx_event['pnl list'].append(fx_event['pnl'])

            # -------- calculate position --------

            if edge >= p1 + 0.03 and lead_pnl_event['lead_limit_check'] < 5 and dom_pnl_event['dom_limit_check'] < 5 and abs(edge - parameters_event['last_edge']) <= 0.3 and parameters_event['switch_bars'] >= 12:  # <= dt.datetime.strptime('23:59:59', '%H:%M:%S')
                # short lead long dom
                parameters_event['p1_signal'] += 1

                while edge >= parameters_event['p1'] + 0.03:
                    if lead_close * lead_pnl_event['position'] * self.lead_mltp >= -max_notion and parameters_event['p1_signal'] > 1 and deal_qty < parameters_event['deal_qty_limit']:
                        # lead qty

                        lead_qty = -quantity
                        lead_pnl_event['position'] += lead_qty
                        lead_pnl_event['pnl'] += -abs(lead_qty * lead_fee)

                        # dom qty - delta neutral

                        dom_qty = round(-(lead_close * lead_pnl_event['position'] * self.lead_mltp) / (dom_close * self.dom_mltp / fx_close), 0) - dom_pnl_event['position']
                        dom_pnl_event['position'] += dom_qty
                        dom_pnl_event['pnl'] += -abs(dom_qty * dom_fee) / fx_close
                        print('#################')

                        # UC qty

                        fx_qty = round((lead_qty * lead_close * self.lead_mltp) / 100000, 1)
                        fx_event['position'] += fx_qty
                        fx_event['pnl'] += -abs(fx_qty) * fx_fee

                        parameters_event['p1'] += skew * quantity

                        trading_button = 1
                        deal_qty += 1

                    else:
                        # lead_pnl_event['position'] = -round(max_notion / (lead_close * self.lead_mltp), 0)
                        # dom_pnl_event['position'] = round(max_notion / (dom_close * self.dom_mltp / fx_close), 0)

                        break

            elif edge <= p2 - 0.03 and lead_pnl_event['lead_limit_check'] < 5 and dom_pnl_event['dom_limit_check'] < 5 and abs(edge - parameters_event['last_edge']) <= 0.3 and parameters_event['switch_bars'] >= 12:  # <= dt.datetime.strptime('23:59:59', '%H:%M:%S')
                # long lead short dom
                parameters_event['p2_signal'] += 1

                while edge <= parameters_event['p1'] - pt - 0.03:

                    if lead_close * lead_pnl_event['position'] * self.lead_mltp <= max_notion and parameters_event['p2_signal'] > 1 and deal_qty < parameters_event['deal_qty_limit']:
                        # lead qty

                        lead_qty = quantity
                        lead_pnl_event['position'] += lead_qty
                        lead_pnl_event['pnl'] += -abs(lead_qty * lead_fee)

                        # dom qty - delta neutral

                        dom_qty = round(-(lead_close * lead_pnl_event['position'] * self.lead_mltp) / (dom_close * self.dom_mltp / fx_close), 0) - dom_pnl_event['position']
                        dom_pnl_event['position'] += dom_qty
                        dom_pnl_event['pnl'] += -abs(dom_qty * dom_fee) / fx_close

                        # UC qty

                        fx_qty = round((lead_qty * lead_close * self.lead_mltp) / 100000, 1)
                        fx_event['position'] += fx_qty
                        fx_event['pnl'] += -abs(fx_qty) * fx_fee

                        parameters_event['p1'] -= skew * quantity

                        trading_button = -1
                        deal_qty += 1

                    else:
                        # lead_pnl_event['position'] = round(max_notion / (lead_close * self.lead_mltp), 0)
                        # dom_pnl_event['position'] = -round(max_notion / (dom_close * self.dom_mltp / fx_close), 0)

                        break

            else:
                parameters_event['p1_signal'] = 0
                parameters_event['p2_signal'] = 0

            #print(dates, time, edge, p1, p2, skew, ema_np[i], lead_pnl_event['position'], dom_pnl_event['position'], lead_close * lead_pnl_event['position'] * self.lead_mltp)
            #print(dates, time, lead_pnl_event['position'], dom_pnl_event['position'], 'edge', edge, 'ema', ema_np[i], p1, p2)

            last_lead_close = lead_pnl_event['price']

            if trading_button == 1:
                lead_pnl_event['price'] = lead_close
                dom_pnl_event['price'] = dom_close

            elif trading_button == -1:
                lead_pnl_event['price'] = lead_close
                dom_pnl_event['price'] = dom_close

            else:
                lead_pnl_event['price'] = lead_close
                dom_pnl_event['price'] = dom_close

            fx_event['price'] = fx_close

            lead_pnl_event['contract'] = lead_contract
            dom_pnl_event['contract'] = dom_contract
            fx_event['contract'] = fx_contract

            parameters_event['last_skew'] = skew
            parameters_event['last_edge'] = edge

            if dates != parameters_event['date']:
                if dates in ['2021-04-30', '2021-06-11']:
                    start_time = "14:55:00"
                    end_time = "14:59:00"

                else:
                    start_time = "22:55:00"
                    end_time = "22:59:00"

                if dt.datetime.strptime(start_time, "%H:%M:%S") <= dt.datetime.strptime(time, "%H:%M:%S") <= dt.datetime.strptime(end_time, "%H:%M:%S"):
                    print('---- start ----')

                    temp_daily_np = np.array(
                        [dates, time, lead_close, lead_contract_np[i], dom_close,
                         dom_contract_np[i], fx_close, lead_pnl_event['position'], dom_pnl_event['position'],
                         lead_close * lead_pnl_event['position'] * self.lead_mltp,
                         dom_close * dom_pnl_event['position'] * self.dom_mltp / fx_close, fx_event['position'], fx_event['pnl'],
                         lead_pnl_event['pnl'] + dom_pnl_event['pnl'], edge_close_np[i], p1, pt, skew, ema_np[i], spread_pct]
                        )

                    daily_list.append(temp_daily_np)
                    # print(temp_daily_np)

                    parameters_event['date'] = dates

            # min pnl

            temp_min_np = np.array(
                [dates, time, lead_close, lead_contract_np[i], dom_close,
                 dom_contract_np[i], fx_close, lead_pnl_event['position'], dom_pnl_event['position'],
                 lead_close * lead_pnl_event['position'] * self.lead_mltp,
                 dom_close * dom_pnl_event['position'] * self.dom_mltp / fx_close, fx_event['position'],
                 fx_event['pnl'], lead_pnl_event['pnl'], dom_pnl_event['pnl'],
                 lead_pnl_event['pnl'] + dom_pnl_event['pnl'], edge_close_np[i], parameters_event['p1_signal'], parameters_event['p2_signal'],
                 p1, pt, skew, ema_np[i], spread_pct, lead_close, last_lead_close, lead_pnl_event['position'], self.lead_mltp]
            )

            min_list.append(temp_min_np)

        daily_pd = pd.DataFrame(np.array(daily_list),
                                columns=['Date', 'time', 'lead price', 'lead contract', 'dom price',
                                         'dom contract', 'fx price', 'lead pos', 'dom pos',
                                         'lead notion', 'dom notion', 'fx pos', 'fx pnl', 'total pnl', 'edge', 'p1', 'pt', 'skew', 'ema', 'spread_pct'])

        min_pd = pd.DataFrame(np.array(min_list),
                                columns=['Date', 'time', 'lead price', 'lead contract', 'dom price',
                                         'dom contract', 'fx price', 'lead pos', 'dom pos',
                                         'lead notion', 'dom notion', 'fx pos', 'fx pnl', 'lead', 'dom', 'total pnl', 'edge', 'p1_trigger_signal', 'p2_trigger_signal', 'p1',
                                         'pt', 'skew', 'ema', 'spread_pct', 'lead_close', 'last lead price', 'lead position', 'lead mltp'])

        # add spread mean and std

        min_pd['spread mean'] = self.data_pd['spread mean']
        min_pd['spread std'] = self.data_pd['spread std']

        # ======== save data =========

        if self.mode == 'pa':
            daily_pd.to_csv(save_daily_data_path % (self.pair_ticker + '_pa'), index=False)
            min_pd.to_csv(save_min_data_path % (self.pair_ticker + '_pa'), index=False)

        else:
            daily_pd.to_csv(save_daily_data_path % (self.pair_ticker + '_' + self.mode), index=False)
            min_pd.to_csv(save_min_data_path % (self.pair_ticker + '_' + self.mode), index=False)

        # -------- plot ---------

        # plt.subplot(3, 1, 1)
        # plt.plot(np.array(lead_pnl_event['pnl list']) + np.array(dom_pnl_event['pnl list'] + np.array(fx_event['pnl list'])), 'g')
        #
        # plt.subplot(3, 1, 2)
        # plt.plot(lead_close_np, 'r')
        #
        # plt.subplot(3, 1, 3)
        # plt.plot(dom_close_np, 'b')
        # plt.show()


if __name__ == '__main__':
    pair_list = ['GC_AU', 'SI_AG', 'HG_CU', 'S_BP', 'BO_SH', 'SM_AE']
    #pair_list = ['SI_AG']

    for pair in pair_list:
        bt = BtFramework(pair, 'bt')
        bt.run_pnl()
