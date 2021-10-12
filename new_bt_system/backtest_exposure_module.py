import numpy as np
import pandas as pd
import datetime as dt
import matplotlib.pyplot as plt

read_min_pnl_path = r'S:\Eric\Commodities\backtest_report\min_report_para\%s.csv'
read_merged_data_path = r'S:\bjin\FutMinData\Commodities\%s\%s.csv'


class BtFramework(object):
    def __init__(self, pair_ticker):
        self.pair_ticker = pair_ticker

        self.lead_ticker = pair_ticker.split('_')[0]
        self.dom_ticker = pair_ticker.split('_')[1]
        self.fx = 'UC'

        self.pnl_min_pd = pd.read_csv(read_min_pnl_path % (self.pair_ticker + '_para_bt'))

        self.total_pnl_np = np.array(self.pnl_min_pd['total pnl'])
        self.fx_pnl_np = np.array(self.pnl_min_pd['fx pnl'])

        self.parameter = pd.read_csv('parameter_param.csv', index_col='pair')
        self.universe = pd.read_csv('universe.csv', index_col='Product')

        self.lead_mltp = self.universe.loc[self.lead_ticker]['Multiplier']
        self.dom_mltp = self.universe.loc[self.dom_ticker]['Multiplier']

        self.lookback_window = int(self.parameter.loc[self.pair_ticker]['lookback_window'])

        # bands parameters

        self.bands_window = 1200
        self.bands_std_range = 1.5

        self.exposure_ratio = 0.2

    def backtesting_cal(self):
        dates_np = np.array(self.pnl_min_pd['Date'])
        time_np = np.array(self.pnl_min_pd['time'])

        lead_pos_np = np.array(self.pnl_min_pd['lead pos'])
        dom_pos_np = np.array(self.pnl_min_pd['dom pos'])
        fx_pos_np = np.array(self.pnl_min_pd['fx pos'])

        lead_contracts_np = np.array(self.pnl_min_pd['lead contract'])
        dom_contracts_np = np.array(self.pnl_min_pd['dom contract'])
        fx_contracts_np = np.array(self.pnl_min_pd['fx contract'])

        lead_notion_np = np.array(self.pnl_min_pd['lead notion'])
        dom_notion_np = np.array(self.pnl_min_pd['dom notion'])

        lead_close_np = np.array(self.pnl_min_pd['lead price'])
        dom_close_np = np.array(self.pnl_min_pd['dom price'])
        fx_close_np = np.array(self.pnl_min_pd['fx price'])

        # exposure strategy requirement

        ema_period = int(self.parameter.loc[self.pair_ticker]['ema'])
        max_notion = self.parameter.loc[self.pair_ticker]['max_notion']

        spread_np = np.array(self.pnl_min_pd['spread_pct'])
        edge_np = np.array(self.pnl_min_pd['edge'])

        ema_np = np.array(self.pnl_min_pd['ema'])
        mean_np = np.array(self.pnl_min_pd['spread mean'])
        std_np = np.array(self.pnl_min_pd['spread std'])

        # ----- start bt -----

        lead_fee = self.universe.loc[self.lead_ticker]['commission']
        dom_fee = self.universe.loc[self.dom_ticker]['commission']
        fx_fee = self.universe.loc[self.fx]['commission']

        lead_pnl_event = {'pnl': 0, 'pnl list': [], 'position': 0, 'position list': [], 'price': 0, 'contract': lead_contracts_np[0],
                          'lead_limit_check': 0}
        dom_pnl_event = {'pnl': 0, 'pnl list': [], 'position': 0, 'position list': [], 'price': 0, 'contract': dom_contracts_np[0],
                         'dom_limit_check': 0}
        fx_pnl_event = {'pnl': 0, 'pnl list': [], 'position': 0, 'position list': [], 'price': fx_close_np[0], 'contract': fx_contracts_np[0]}

        parameters_event = {'switch_bars': 0, 'last_day_end_index': 0, 'trading_signal': 'off',
                            'last_lead_contract': lead_contracts_np[0],
                            'last_dom_contract': dom_contracts_np[0],
                            'last_fx_contract': fx_contracts_np[0]}

        for index in range(len(time_np)):
            date = dates_np[index]
            time = time_np[index]
            edge = edge_np[index]

            spread = spread_np[index]
            spread_mean = mean_np[index]
            spread_std = std_np[index]

            lead_contract = lead_contracts_np[index]
            dom_contract = dom_contracts_np[index]
            fx_contract = fx_contracts_np[index]

            lead_pos = lead_pos_np[index]
            dom_pos = dom_pos_np[index]
            fx_pos = fx_pos_np[index]

            lead_close = lead_close_np[index]
            dom_close = dom_close_np[index]
            fx_close = fx_close_np[index]

            lead_notion = lead_notion_np[index]

            # ------- price limit check -------

            if lead_close == lead_pnl_event['price']:
                lead_pnl_event['lead_limit_check'] += 1

            else:
                lead_pnl_event['lead_limit_check'] = 0

            if dom_close == dom_pnl_event['price']:
                dom_pnl_event['dom_limit_check'] += 1

            else:
                dom_pnl_event['dom_limit_check'] = 0

            # ------- switch contract -------

            if index == 0 \
                    or (lead_contract != parameters_event['last_lead_contract']) \
                    or (dom_contract != parameters_event['last_dom_contract']) \
                    or (fx_contract != parameters_event['last_fx_contract']):

                parameters_event['switch_bars'] = 0

                lead_pnl_event['position'] = 0
                dom_pnl_event['position'] = 0
                fx_pnl_event['position'] = 0

            else:
                parameters_event['switch_bars'] += 1

            # -------- calculate this tick pnl ---------

            lead_pnl_event['pnl'] += (lead_close - lead_pnl_event['price']) * lead_pnl_event[
                'position'] * self.lead_mltp
            lead_pnl_event['pnl list'].append(lead_pnl_event['pnl'])

            dom_pnl_event['pnl'] += (dom_close / fx_close - dom_pnl_event['price'] / fx_pnl_event['price']) * dom_pnl_event[
                'position'] * self.dom_mltp
            dom_pnl_event['pnl list'].append(dom_pnl_event['pnl'])

            fx_pnl_event['pnl'] += ((fx_close - fx_pnl_event['price']) / fx_close) * 100000 * fx_pnl_event['position']
            fx_pnl_event['pnl list'].append(fx_pnl_event['pnl'])

            # -------- check the signal --------

            if abs(lead_notion) >= 0.9 * max_notion:
                if spread >= spread_mean + self.bands_std_range * spread_std or spread <= spread_mean - spread_std:
                    exposure_lead_pos = -int(self.exposure_ratio * lead_pos)
                    exposure_dom_pos = -int(exposure_lead_pos / lead_pos * dom_pos)
                    exposure_fx_pos = 2 * int(exposure_lead_pos / lead_pos * fx_pos)

                    print(date, time, lead_pos, dom_pos, exposure_lead_pos, exposure_dom_pos)

                else:
                    exposure_lead_pos = lead_pnl_event['position']
                    exposure_dom_pos = dom_pnl_event['position']
                    exposure_fx_pos = fx_pnl_event['position']

            elif 0.9 * max_notion > abs(lead_notion) >= 0.5 * max_notion:
                if spread >= spread_mean + self.bands_std_range * spread_std or spread <= spread_mean - spread_std:
                    # exposure_lead_pos = -int(self.exposure_ratio * lead_pos)
                    # exposure_dom_pos = -int(exposure_lead_pos / lead_pos * dom_pos)
                    # exposure_fx_pos = 2 * int(exposure_lead_pos / lead_pos * fx_pos)

                    exposure_lead_pos = 0
                    exposure_dom_pos = 0
                    exposure_fx_pos = 0

                else:
                    exposure_lead_pos = 0
                    exposure_dom_pos = 0
                    exposure_fx_pos = 0

            else:
                exposure_lead_pos = 0
                exposure_dom_pos = 0
                exposure_fx_pos = 0

            # -------- calculate new position pnl ----------

            if lead_pnl_event['position'] != exposure_lead_pos or dom_pnl_event['position'] != exposure_dom_pos or fx_pnl_event['position'] != exposure_fx_pos:
                lead_qty = exposure_lead_pos - lead_pnl_event['position']
                lead_pnl_event['position'] += lead_qty
                lead_pnl_event['pnl'] += -abs(lead_qty * lead_fee)

                dom_qty = exposure_dom_pos - dom_pnl_event['position']
                dom_pnl_event['position'] += dom_qty
                dom_pnl_event['pnl'] += -abs(dom_qty * dom_fee) / fx_close

                fx_qty = exposure_fx_pos - fx_pnl_event['position']
                fx_pnl_event['position'] += fx_qty
                fx_pnl_event['pnl'] += -abs(fx_qty * fx_fee)

            else:
                # hold
                pass

            # signal generation and position adjustment

            if time == '14:59:00':
                if index != parameters_event['last_day_end_index']:
                    pass

                else:
                    # first day
                    pass

                # update parameters

                parameters_event['last_day_end_index'] = index

            parameters_event['last_lead_contract'] = lead_contract
            parameters_event['last_dom_contract'] = dom_contract
            parameters_event['last_fx_contract'] = fx_contract

            lead_pnl_event['position list'].append(lead_pnl_event['position'])
            dom_pnl_event['position list'].append(dom_pnl_event['position'])
            fx_pnl_event['position list'].append(fx_pnl_event['position'])

            lead_pnl_event['price'] = lead_close
            dom_pnl_event['price'] = dom_close
            fx_pnl_event['price'] = fx_close

        lead_pnl_np = np.array(lead_pnl_event['pnl list'])
        dom_pnl_np = np.array(dom_pnl_event['pnl list'])
        fx_pnl_np = np.array(fx_pnl_event['pnl list'])

        # save data

        total_pnl_np = lead_pnl_np + dom_pnl_np + fx_pnl_np

        plt.subplot(2, 1, 1)
        plt.plot(total_pnl_np, 'r')
        # plt.plot(lead_pnl_np, 'r')
        # plt.plot(dom_pnl_np, 'y')
        # plt.plot(fx_pnl_np, 'm')
        plt.plot(self.total_pnl_np + self.fx_pnl_np, 'b')
        plt.plot(self.total_pnl_np + self.fx_pnl_np + total_pnl_np, 'g')

        plt.subplot(2, 1, 2)
        plt.plot(mean_np, 'g')
        plt.plot(spread_np, 'b')

        plt.plot(mean_np + self.bands_std_range * std_np, 'r')
        plt.plot(mean_np - self.bands_std_range * std_np, 'r')
        plt.show()

    def basis_spread_analysis(self):
        lead_data1 = pd.read_csv(read_merged_data_path % (self.lead_ticker, self.lead_ticker + '1')).\
                rename(columns={"Close": "lead_close1", "Open": "lead_open1",  "High": "lead_high1", "Low": "lead_low1", "Contract": "lead_contract1"})
        lead_data2 = pd.read_csv(read_merged_data_path % (self.lead_ticker, self.lead_ticker + '2')).\
                rename(columns={"Close": "lead_close2", "Open": "lead_open2",  "High": "lead_high2", "Low": "lead_low2", "Contract": "lead_contract2"})

        dom_data1 = pd.read_csv(read_merged_data_path % (self.dom_ticker, self.dom_ticker + '1')).\
                    rename(columns={"Close": "dom_close1", "Open": "dom_open1", "High": "dom_high1", "Low": "dom_low1", "Contract": "dom_contract1"})
        dom_data2 = pd.read_csv(read_merged_data_path % (self.dom_ticker, self.dom_ticker + '2')).\
                    rename(columns={"Close": "dom_close2", "Open": "dom_open2", "High": "dom_high2", "Low": "dom_low2", "Contract": "dom_contract2"})

        lead_data_continuous = pd.merge(lead_data1, lead_data2, on='time')
        dom_data_continuous = pd.merge(dom_data1, dom_data2, on='time')

        lead_data_continuous['lead basis spread'] = lead_data_continuous['lead_close1'] - lead_data_continuous['lead_close2']
        dom_data_continuous['dom basis spread'] = dom_data_continuous['dom_close1'] - dom_data_continuous['dom_close2']

        merge_data_continuous = pd.merge(lead_data_continuous, dom_data_continuous, on='time')

        plt.subplot(3, 1, 1)
        plt.plot(merge_data_continuous['lead basis spread'])

        plt.subplot(3, 1 ,2)
        plt.plot(merge_data_continuous['dom basis spread'])

        plt.subplot(3, 1, 3)
        plt.plot(self.total_pnl_np + self.fx_pnl_np, 'r')

        plt.show()


if __name__ == '__main__':
    bt = BtFramework('S_BP')
    bt.backtesting_cal()