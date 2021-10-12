import numpy as np
import pandas as pd
import datetime as dt
import matplotlib.pyplot as plt


class ParametersCalculation(object):
    def __init__(self, pair_ticker, data_pd):
        self.pair_ticker = pair_ticker
        self.lead_ticker = pair_ticker.split('_')[0]
        self.dom_ticker = pair_ticker.split('_')[1]
        self.fx = 'UC'

        self.data_pd = data_pd

        # skew algo parameters

        self.spread_pct_np = []
        self.edge_np = []
        self.ema_np = []

        self.time_np = np.array(self.data_pd['time'])
        self.lead_close_np = np.array(self.data_pd['lead_close'])
        self.dom_close_np = np.array(self.data_pd['dom_close'])
        self.fx_close_np = np.array(self.data_pd['fx_close'])

        # ======================

        self.lead_contract_np = np.array(self.data_pd['lead_contract'])
        self.dom_contract_np = np.array(self.data_pd['dom_contract'])
        self.fx_contract_np = np.array(self.data_pd['fx_contract'])

        self.parameter = pd.read_csv('parameter_param.csv', index_col='pair')
        self.universe = pd.read_csv('universe.csv', index_col='Product')

        self.ema_period = self.parameter.loc[self.pair_ticker]['ema']
        self.lead_unit = self.parameter.loc[self.pair_ticker]['lead_unit']
        self.dom_unit = self.parameter.loc[self.pair_ticker]['dom_unit']
        self.lead_mltp = self.universe.loc[self.lead_ticker]['Multiplier']
        self.dom_mltp = self.universe.loc[self.dom_ticker]['Multiplier']

    def ema_cal(self):
        spread_pd = (self.lead_close_np * (self.lead_mltp * self.lead_unit * self.fx_close_np) - (self.dom_close_np * self.dom_mltp * self.dom_unit))
        spread_pct_pd = spread_pd * 100 / (self.dom_close_np * self.dom_mltp * self.dom_unit)

        ema_event = {'ema_list': [], 'last_ema': 0, 'last_lead_contract': self.lead_contract_np[0],
                     'last_dom_contract': self.dom_contract_np[0], 'last_fx_contract': self.fx_contract_np[0]}

        spread_event = {'lookback_window': 3000, 'switch_num': 0, 'cal_num': 500, 'last_std': 0, 'last_mean': 0,
                        'mean_list': [], 'std_list': []}

        spread_pct_np = np.array(spread_pct_pd)
        hv_np = np.array(realized_vol_cal(spread_pct_np, 4500))

        print('######## EMA calculation ########')

        for index in range(len(self.time_np)):
            lead_contract = self.lead_contract_np[index]
            dom_contract = self.dom_contract_np[index]
            fx_contract = self.fx_contract_np[index]

            spread_pct = spread_pct_np[index]
            alpha = 2 / (self.ema_period + 1)

            # ema cal

            if index == 0 or (lead_contract != ema_event['last_lead_contract']) or (dom_contract != ema_event['last_dom_contract']) \
                    or (fx_contract != ema_event['last_fx_contract']):

                print('######## ', self.time_np[index], lead_contract, dom_contract, fx_contract, ' ########')

                temp_ema = spread_pct

                spread_event['switch_num'] = 0

                spread_mean = spread_pct
                spread_std = 1

                spread_event['last_mean'] = spread_mean
                spread_event['last_std'] = spread_std

            else:
                spread_event['switch_num'] += 1

                temp_ema = alpha * spread_pct + (1 - alpha) * ema_event['last_ema']

                # calculate spread mean and std

                if spread_event['switch_num'] > spread_event['lookback_window']:
                    temp_spread_pct_np = spread_pct_np[index - spread_event['lookback_window'] + 1: index + 1]

                    spread_mean = np.mean(temp_spread_pct_np)
                    spread_std = np.std(temp_spread_pct_np)

                else:
                    if spread_event['switch_num'] > spread_event['cal_num']:
                        temp_spread_pct_np = spread_pct_np[index - spread_event['switch_num'] + 1: index + 1]

                        spread_mean = np.mean(temp_spread_pct_np)
                        spread_std = np.std(temp_spread_pct_np)

                    else:
                        spread_mean = temp_ema
                        spread_std = spread_event['last_std']

                spread_event['last_mean'] = spread_mean
                spread_event['last_std'] = spread_std

            # update ema event

            ema_event['last_lead_contract'] = lead_contract
            ema_event['last_dom_contract'] = dom_contract
            ema_event['last_fx_contract'] = fx_contract

            ema_event['last_ema'] = temp_ema
            ema_event['ema_list'] .append(temp_ema)

            spread_event['mean_list'].append(spread_event['last_mean'])
            spread_event['std_list'].append(spread_event['last_std'])

        self.data_pd['spread pct'] = spread_pct_np
        self.data_pd['spread'] = spread_pd
        self.data_pd['ema'] = np.array(ema_event['ema_list'])
        self.data_pd['edge'] = spread_pct_np - np.array(ema_event['ema_list'])
        self.data_pd['hv'] = hv_np
        self.data_pd['spread mean'] = np.array(spread_event['mean_list'])
        self.data_pd['spread std'] = np.array(spread_event['std_list'])

        # plt.subplot(2, 1, 1)
        # plt.plot(self.data_pd['spread pct'])
        # plt.plot(self.data_pd['spread mean'])
        # plt.plot(np.array(self.data_pd['spread mean']) + 1.5 * np.array(self.data_pd['spread std']))
        # plt.plot(np.array(self.data_pd['spread mean']) - 1.5 * np.array(self.data_pd['spread std']))
        #
        # plt.subplot(2, 1, 2)
        # plt.plot(self.data_pd['edge'])
        #
        # plt.show()

        print('######## EMA Calculation Done !!! ########')

        # ============ save data_pd as csv file =============

        self.data_pd.to_csv('ema_data/' + self.pair_ticker + '_ema_data.csv', index=False)

        print('######## EMA Data Saved !!! ########')

        # ============ parameters update =============

        # skew algo parameters

        self.spread_pct_np = np.array(self.data_pd['spread pct'])
        self.edge_np = np.array(self.data_pd['edge'])
        self.ema_np = np.array(self.data_pd['ema'])

        # ============ END ============

    def ema_window_cal(self, algo):
        if algo == 'static':
            pass

        elif algo == 'dynamic':
            pass

        else:
            print('no this algo')
            exit()

    def pt_skew_cal(self, algo):
        print('######## pt & skew calculation !!! ########')

        if algo == 'const':
            pt = self.parameter.loc[self.pair_ticker]['pt']
            skew = self.parameter.loc[self.pair_ticker]['skew']

            self.data_pd['skew'] = skew
            self.data_pd['pt'] = pt

        elif algo == 'amplitude_control':
            lookback_window = int(self.parameter.loc[self.pair_ticker]['lookback_window'])
            max_notional = self.parameter.loc[self.pair_ticker]['max_notion']

            qty = self.parameter.loc[self.pair_ticker]['qty']

            limit_skew = self.parameter.loc[self.pair_ticker]['skew']
            limit_pt = self.parameter.loc[self.pair_ticker]['pt']

            skew_event = {'skew_list': [], 'pt_list': [], 'updated_skew': limit_skew, 'updated_pt': limit_pt, 'switch_num': 0,
                          'last_lead_contract': self.lead_contract_np[0],
                          'last_dom_contract': self.dom_contract_np[0],
                          'last_fx_contract': self.fx_contract_np[0]}

            for index in range(len(self.time_np)):
                date = self.time_np[index].split(' ')[0]
                time = self.time_np[index].split(' ')[1]

                close = self.lead_close_np[index]

                lead_contract = self.lead_contract_np[index]
                dom_contract = self.dom_contract_np[index]
                fx_contract = self.fx_contract_np[index]

                spread_pct = self.spread_pct_np[index]
                edge = self.edge_np[index]
                ema = self.ema_np[index]

                unit_notional = close * self.universe.loc[self.lead_ticker]['Multiplier']
                max_qty = np.ceil(max_notional / unit_notional)

                criteria_edge = 0.5 * skew_event['updated_pt'] + skew_event['updated_skew'] * max_qty

                # switch contract

                if index == 0 \
                        or (lead_contract != skew_event['last_lead_contract']) \
                        or (dom_contract != skew_event['last_dom_contract']) \
                        or (fx_contract != skew_event['last_fx_contract']):

                    # print('######## ', self.time_np[index], lead_contract, dom_contract, fx_contract, ' ########')

                    skew_event['switch_num'] = 0

                else:
                    skew_event['switch_num'] += 1

                # update at 14:59:00

                if time == '14:59:00':
                    # amplitude algo

                    if index < lookback_window - 1:
                        temp_spread_pct_np = self.spread_pct_np[0: index]
                        temp_edge_np = self.edge_np[0: index]

                        amplitude = max(temp_spread_pct_np) - min(temp_spread_pct_np)

                        # =========== amplitude control algo ===========
                        # apply amplitude to get reflected skew and pt

                        reflected_skew = np.round(0.5 * (amplitude - skew_event['updated_pt']) / max_qty, 2)

                        if reflected_skew <= limit_skew:
                            reflected_skew = limit_skew

                        reflected_pt = np.round((1 + 0.2 * (reflected_skew / limit_skew)) * limit_pt, 2)

                        if reflected_pt <= limit_pt or reflected_skew == limit_skew:
                            reflected_pt = limit_pt

                        reflected_criteria_edge = 0.5 * skew_event['updated_pt'] + reflected_skew * max_qty

                        # ========== algo end ===========

                        lookback_edge = max(abs(max(temp_edge_np)), abs(min(temp_edge_np)))

                    else:
                        if skew_event['switch_num'] < lookback_window - 1:
                            temp_spread_pct_np = self.spread_pct_np[index - skew_event['switch_num']: index]
                            temp_edge_np = self.edge_np[index - skew_event['switch_num']: index]

                            amplitude = max(temp_spread_pct_np) - min(temp_spread_pct_np)

                            # =========== amplitude control algo ===========
                            # apply amplitude to get reflected skew and pt

                            reflected_skew = np.round(0.5 * (amplitude - skew_event['updated_pt']) / max_qty, 2)

                            if reflected_skew <= limit_skew:
                                reflected_skew = limit_skew

                            reflected_pt = np.round((1 + 0.2 * (reflected_skew / limit_skew)) * limit_pt, 2)

                            if reflected_pt <= limit_pt or reflected_skew == limit_skew:
                                reflected_pt = limit_pt

                            reflected_criteria_edge = 0.5 * skew_event['updated_pt'] + reflected_skew * max_qty

                            # ========== algo end ===========

                            lookback_edge = max(abs(max(temp_edge_np)), abs(min(temp_edge_np)))

                        else:
                            temp_spread_pct_np = self.spread_pct_np[index - lookback_window + 1: index]
                            temp_edge_np = self.edge_np[index - lookback_window + 1: index]

                            amplitude = max(temp_spread_pct_np) - min(temp_spread_pct_np)

                            # =========== amplitude control algo ===========
                            # apply amplitude to get reflected skew and pt

                            reflected_skew = np.round(0.5 * (amplitude - skew_event['updated_pt']) / max_qty, 2)

                            if reflected_skew <= limit_skew:
                                reflected_skew = limit_skew

                            reflected_pt = np.round((1 + 0.2 * (reflected_skew / limit_skew)) * limit_pt, 2)

                            if reflected_pt <= limit_pt or reflected_skew == limit_skew:
                                reflected_pt = limit_pt

                            reflected_criteria_edge = 0.5 * skew_event['updated_pt'] + reflected_skew * max_qty

                            # ========== algo end ===========

                            lookback_edge = max(abs(max(temp_edge_np)), abs(min(temp_edge_np)))

                    print(date)
                    print('amplitude', amplitude, 'max notion', criteria_edge, 'reflected max notion',
                          reflected_criteria_edge, lookback_edge)

                    # skew and pt policy

                    if lookback_edge >= criteria_edge and skew_event['switch_num'] >= 500:
                        print('====== over criteria ======')

                        # update skew and pt

                        skew_event['updated_skew'] = reflected_skew
                        skew_event['updated_pt'] = reflected_pt

                    else:
                        # check if spread converge

                        if lookback_edge < reflected_criteria_edge and skew_event['switch_num'] >= 500:
                            # update skew and pt

                            skew_event['updated_skew'] = reflected_skew
                            skew_event['updated_pt'] = reflected_pt

                        else:
                            pass

                else:
                    pass

                # update ema event

                skew_event['last_lead_contract'] = lead_contract
                skew_event['last_dom_contract'] = dom_contract
                skew_event['last_fx_contract'] = fx_contract

                skew_event['skew_list'].append(skew_event['updated_skew'])
                skew_event['pt_list'].append(skew_event['updated_pt'])

            # update data pd

            self.data_pd['skew'] = np.array(skew_event['skew_list'])
            self.data_pd['pt'] = np.array(skew_event['pt_list'])

            # save latest skew and pt

            pair = self.pair_ticker
            time = self.time_np[-1]
            skew = skew_event['updated_skew']
            pt = skew_event['updated_pt']

            updated_param_pd = pd.read_csv('updated_info/updated_parameters.csv')
            pair_index_list = np.where(updated_param_pd['Pair'] == pair)[0]

            if len(pair_index_list) == 0:
                new_param_pd = pd.DataFrame(np.array([[pair, time, skew, pt]]), columns=['Pair', 'Time', 'skew', 'pt'])

                updated_param_pd = updated_param_pd.append(new_param_pd, ignore_index=True)

                # save to csv file

                updated_param_pd.to_csv('updated_info/updated_parameters.csv', index=False)
            else:
                pair_index = pair_index_list[0]
                updated_param_pd.loc[pair_index, 'skew'] = skew
                updated_param_pd.loc[pair_index, 'pt'] = pt

                updated_param_pd.to_csv('updated_info/updated_parameters.csv', index=False)

            # self.data_pd['pt'] = pt

        else:
            print('no this algo')
            exit()

        print('######## pt & skew calculation Done!!! ########')

    def param_cal(self, algo):
        """
        algo = symmetric, skew

        algo = symmetric: p1 = 0.5 * pt, p2 = -0.5 * pt
        algo = skew: p1 = 0.5 * pt + const, p2 = -0.5 * pt + const

        """

        # call pt and skew array
        print('######## param calculation !!! ########')

        try:
            skew_np = np.array(self.data_pd['skew'])
            pt_np = np.array(self.data_pd['pt'])

        except KeyError:
            #self.data_pd['pt'] = self.parameter.loc[self.pair_ticker]['pt']
            # self.data_pd['skew'] = self.parameter.loc[self.pair_ticker]['skew']

            pt_np = np.array(self.data_pd['pt'])

        # main algo

        if algo == 'symmetric':
            self.data_pd['p1'] = 0.5 * pt_np
            self.data_pd['quantity'] = self.parameter.loc[self.pair_ticker]['qty']
            self.data_pd['notional'] = self.parameter.loc[self.pair_ticker]['max_notion']

        elif algo == 'skew':
            pass

        else:
            print('no this algo')
            exit()

        print('######## param calculation Done!!! ########')


def realized_vol_cal(data, window):
    data_pd = pd.DataFrame(data)
    hv = np.array(data_pd.pct_change().rolling(window).std().fillna(method='bfill')) * np.sqrt(252) * 100

    return hv



if __name__ == '__main__':
    pair_ticker = 'SI_AG'
    test = Test(pair_ticker)

    test.test_skew()
    # data_pd = pd.DataFrame([])

