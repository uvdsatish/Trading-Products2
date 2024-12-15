# Run this after you update the actual entry and adjust the stage to 'post'

import pandas as pd
import psycopg2
import sys
import time
import numpy as np
#import datetime
#from datetime import datetime, timedelta


def read_input_data(excel_file):
    # read excel file in to pandas data frame- which has tradeID, Pyramid#, status, ticker, direction, entry_date, final_exit date,atr, entry price, ops level, target 1, target 2, final exit price, trade duration, T-$, T-R
    excel_df = pd.read_excel(excel_file)

    selected_columns = ['S.No', 'Stage', 'Entry_Date','Ticker','Direction','Stock_reading', 'Pyramid', 'Entry_Stop', 'OPS', 'Base_level', 'BO_level', 'Filter_target', 'BG_target', 'Trigger_target',  'Trade_Risk', 'ATR','Capital','Risk','UnitR','Risk_per',	'Entry_Limit',	'Target_1',	'Target_2',	'R_R1',	'R_R2',	'R_R3','R_R4','R_R5','oneR_level','twoR_level','Peel_Seed','Quantity','Half_Q', 'OPS_Per','OPS_ATR','Capital_outlay','Entry_price']

    excel_df = excel_df[selected_columns]

    condition = (excel_df['Stage'] == "Post")

    filtered_df = excel_df[condition]

    return filtered_df




def calculate_metrics(in_df):

    res_series = in_df.apply(get_ticker_metrics_data, axis=1, result_type='expand')

    out_df = pd.concat([in_df, res_series], axis=1)

    return out_df


def get_ticker_metrics_data(row):




    # Directional values


    if row['Direction'] == "Long":
        A_UnitR = (row['Entry_price'] - row['OPS'])
        A_1R = (row['Entry_price'] + A_UnitR)
        A_2R = (row['Entry_price'] + 2*A_UnitR)
        A_R_R2 = (row['Target_2'] - row['Entry_Stop']) / A_UnitR
    elif row['Direction'] == "Short":
        A_UnitR = -(row['Entry_price'] - row['OPS'])
        A_1R = (row['Entry_price'] - A_UnitR)
        A_2R = (row['Entry_price'] - 2 * A_UnitR)
        A_R_R2 = -(row['Target_2'] - row['Entry_Stop']) / A_UnitR
    else:
        A_UnitR = "Incorrect Direction - has to be Long or Short"
        A_1R = "Incorrect Direction - has to be Long or Short"
        A_2R = "Incorrect Direction - has to be Long or Short"
        A_R_R2 = "Incorrect Direction - has to be Long or Short"

    # Derived values

    Adj_Quantity = row['Risk'] / A_UnitR
    Quantity_delta = Adj_Quantity - row['Quantity']
    A_Risk = row['Quantity'] * A_UnitR
    A_Capital_outlay = row['Quantity'] * row['Entry_price']
    A_OPS_Per = (A_UnitR/row['Entry_price'])*100
    A_OPS_ATR = (A_UnitR/row['ATR'])



    metrics_list = [A_UnitR, A_1R, A_2R, A_R_R2, A_OPS_Per,	A_OPS_ATR, Quantity_delta, A_Risk, A_Capital_outlay]

    index_names = ['A_UnitR', 'A_1R', 'A_2R', 'A_R_R2', 'A_OPS_Per', 'A_OPS_ATR', 'Quantity_delta', 'A_Risk', 'A_Capital_outlay']

    metrics_series = pd.Series(metrics_list, index=index_names)

    return metrics_series





def update_excel(excel_file,df):
    # Output file has Prices after entry and exit date: 1 ,2, 3; 5; 10; 15; 22; 44; 66 (so 18 prices in total) - we will calculate the change w.r.t risk (R) and daily ATR (A)
    # Also, we will calculate MAE, MFE for the holding period and twice the holding period - necessary for the calculating the capture rate

    in_df =  pd.read_excel(excel_file)

    in_df = in_df.append(df, ignore_index=True)
    in_df.drop_duplicates(subset='S.No', keep='last', inplace=True)

    in_df = in_df.round(2)

    in_df.to_excel(excel_file, index=False)





if __name__ == '__main__':

    # record start time
    start = time.time()


    in_file = r"D:\Trading Dropbox\Satish Udayagiri\SatishUdayagiri\Trading\Process\Daily_Task\daily_work\Tradelog_Automated.xlsx"


    in_df = read_input_data(in_file)

    out_df = calculate_metrics(in_df)


    update_excel(in_file, out_df)



    # record end time
    end = time.time()

    # print the difference between start
    # and end time in milli. secs
    print("The time of execution of above program is :",
          ((end - start) * 10 ** 3) / 60000, "minutes")
