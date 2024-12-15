# Expand the tradelog from the initial set of entries; Pre - prior to trade entry; Post - after the trade triggered; NT* - Not triggered; Perf - active trades - ready to go to month\
import pandas as pd
import psycopg2
import sys
import time


def connect(params_dic):
    """ Connect to the PostgreSQL database server """
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params_dic)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1)
    print("Connection successful")
    return conn



def read_input_data(excel_file):
    # read excel file in to pandas data frame- which has tradeID, Pyramid#, status, ticker, direction, entry_date, final_exit date,atr, entry price, ops level, target 1, target 2, final exit price, trade duration, T-$, T-R
    excel_df = pd.read_excel(excel_file)

    selected_columns = ['S.No', 'Stage', 'Entry_Date','Ticker','Direction','Stock_reading', 'Pyramid', 'Entry_Stop', 'OPS', 'Base_level', 'BO_level', 'Filter_target', 'BG_target', 'Trigger_target',  'Trade_Risk']

    excel_df = excel_df[selected_columns]

    condition = (excel_df['Stage'] == "Pre")

    filtered_df = excel_df[condition]

    return filtered_df


def get_tickers_data_df(con, df):
    # Get unique tickers all prices
    tickers = df['Ticker'].unique()

    print("getting all prices for tickers")
    # Construct query to get all prices data in one query
    query = """
        SELECT ticker, date, open, high, low, close, atr14 
        FROM latest_indicators_view
        WHERE ticker IN %(tkrs)s
    """

    query_params = {
        'tkrs': tuple(tickers),
    }

    # Execute query
    tickers_df = pd.read_sql(query, con, params=query_params)


    return tickers_df



def calculate_metrics(in_df,dct, tickers_df):

    res_series = in_df.apply(get_ticker_metrics_data, axis=1, result_type='expand', args=(dct,tickers_df,))

    out_df = pd.concat([in_df, res_series], axis=1)

    return out_df

def get_atr(ticker, tickers_df):

    filtered = tickers_df.query("ticker == @ticker")
    atr = filtered.iloc[0]['atr14']

    return atr

def get_ticker_metrics_data(row, dct, tickers_df):

    ATR = get_atr(row['Ticker'], tickers_df)

    # Non-Directional values
    Capital = dct['capital']

    if row['Trade_Risk'] == 'long_mtf':
        Risk = dct['long_mtf']
    elif row['Trade_Risk'] == 'long_mta':
        Risk = dct['long_mta']
    elif row['Trade_Risk'] == 'short_mtf':
        Risk = dct['short_mtf']
    elif row['Trade_Risk'] == 'short_mta':
        Risk = dct['short_mta']
    elif row['Trade_Risk'] == 'proven_intuition':
        Risk = dct['proven_intuition']
    elif row['Trade_Risk'] == 'test_intuition':
        Risk = dct['test_intuition']
    elif row['Trade_Risk'] == 'pilot_trade':
        Risk = dct['pilot_trade']
    else:
        Risk = "Incorrect Trade_Risk value"

    Risk_per = (Risk / Capital) * 100


    # Directional values


    if row['Direction'] == "Long":
        UnitR = (row['Entry_Stop'] - row['OPS'])
        Entry_Limit = (row['OPS'] + 0.75*ATR)
        if Entry_Limit < row['Entry_Stop']:
            Entry_Limit = row['Entry_Stop']+0.05
        Target_1 = row['BO_level'] + ( row['BO_level'] - row['Base_level'])
        Target_2 = row['BO_level'] + 2*(row['BO_level'] - row['Base_level'])
        oneR_level = (row['Entry_Stop'] + UnitR)
        twoR_level = (row['Entry_Stop'] + 2*UnitR)
        R_R1 = (Target_1 - row['Entry_Stop'])/UnitR
        R_R2 = (Target_2 - row['Entry_Stop']) / UnitR
        R_R3 = (row['Filter_target'] - row['Entry_Stop']) / UnitR
        R_R4 = (row['BG_target'] - row['Entry_Stop']) / UnitR
        R_R5 = (row['Trigger_target'] - row['Entry_Stop']) / UnitR
    elif row['Direction'] == "Short":
        UnitR = -(row['Entry_Stop'] - row['OPS'])
        Entry_Limit = (row['OPS'] - 0.75 * ATR)
        if Entry_Limit > row['Entry_Stop']:
            Entry_Limit = row['Entry_Stop']-0.05
        Target_1 = row['BO_level'] - ( row['Base_level']- row['BO_level'])
        Target_2 = row['BO_level'] - 2 * ( row['Base_level']- row['BO_level'])
        oneR_level = (row['Entry_Stop'] - UnitR)
        twoR_level = (row['Entry_Stop'] - 2 * UnitR)
        R_R1 = -(Target_1 - row['Entry_Stop']) / UnitR
        R_R2 = -(Target_2 - row['Entry_Stop']) / UnitR
        R_R3 = -(row['Filter_target'] - row['Entry_Stop']) / UnitR
        R_R4 = -(row['BG_target'] - row['Entry_Stop']) / UnitR
        R_R5 = -(row['Trigger_target'] - row['Entry_Stop']) / UnitR
    else:
        UnitR = "Incorrect Direction - has to be Long or Short"
        Entry_Limit = "Incorrect Direction - has to be Long or Short"
        Target_1 = "Incorrect Direction - has to be Long or Short"
        Target_2 = "Incorrect Direction - has to be Long or Short"

    # Derived values

    Quantity = Risk / UnitR
    Half_Q = Quantity * 0.5
    OPS_Per = (UnitR/row['Entry_Stop'])*100
    OPS_ATR = (UnitR/ATR)
    Capital_outlay = Quantity * row['Entry_Stop']

    if row['Trigger_target'] > oneR_level:
        Peel_Seed = row['Trigger_target']-0.1*ATR
    else:
        Peel_Seed = oneR_level


    metrics_list = [ATR, Capital, Risk, UnitR, Risk_per, Entry_Limit, Target_1, Target_2, R_R1, R_R2, R_R3, R_R4, R_R5, oneR_level, twoR_level, Peel_Seed, Quantity, Half_Q, OPS_Per, OPS_ATR, Capital_outlay]
    index_names = ['ATR', 'Capital', 'Risk', 'UnitR', 'Risk_per', 'Entry_Limit', 'Target_1', 'Target_2', 'R_R1', 'R_R2', 'R_R3', 'R_R4', 'R_R5', 'oneR_level', 'twoR_level', 'Peel_Seed', 'Quantity', 'Half_Q', 'OPS_Per', 'OPS_ATR', 'Capital_outlay']

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

    host = "127.0.0.1"  # Localhost
    port = 9100  # Historical data socket port


    param_dic = {
        "host": "localhost",
        "database": "Plurality",
        "user": "postgres",
        "password": "root"
    }

    const_dict = {
        "capital": 400000,
        "long_mtf": 2000,
        "long_mta": 1500,
        "short_mtf": 1000,
        "short_mta": 1000,
        "proven_intuition": 2500,
        "test_intuition": 1000,
        "pilot_trade": 500
    }


    in_file = r"D:\Trading Dropbox\Satish Udayagiri\SatishUdayagiri\Trading\Process\Daily_Task\daily_work\Tradelog_Automated.xlsx"

    con = connect(param_dic)
    in_df = read_input_data(in_file)
    ticker_df=get_tickers_data_df(con,in_df)
    out_df = calculate_metrics(in_df, const_dict, ticker_df)


    update_excel(in_file, out_df)

    con.close()

    # record end time
    end = time.time()

    # print the difference between start
    # and end time in milli. secs
    print("The time of execution of above program is :",
          ((end - start) * 10 ** 3) / 60000, "minutes")

