# This script is to get fundamental data - doesn't seem to be working as is now
import pandas as pd

import pyiqfeed.pyiqfeed as iq
from pyiqfeed.localconfig.passwords import dtn_product_id, dtn_login, dtn_password
import datetime

def launch_service():

    svc = iq.FeedService(product=dtn_product_id,
                         version="Debugging",
                         login=dtn_login,
                         password=dtn_password)

    svc.launch(headless=False)

    # If you are running headless comment out the line above and uncomment
    # the line below instead. This runs IQFeed.exe using the xvfb X Framebuffer
    # server since IQFeed.exe runs under wine and always wants to create a GUI
    # window.
    # svc.launch(headless=True)

def get_daily_data_betweenDates(ticker: str, bgn_date: datetime.date, end_date:datetime.date):
    # Historical Daily Data
    hist_conn = iq.HistoryConn(name="pyiqfeed-Example-daily-data")
    hist_listener = iq.VerboseIQFeedListener("History Bar Listener")
    hist_conn.add_listener(hist_listener)

    with iq.ConnConnector([hist_conn]) as connector:
        try:
            daily_data = hist_conn.request_daily_data_for_dates(ticker,bgn_date,end_date)
            print(daily_data)
            cols = ["Timestamp", "High", "Low", "Open", "Close", "Volume", "Open Interest"]
            df = pd.DataFrame.from_records(daily_data, columns = cols)
        except (iq.NoDataError, iq.UnauthorizedError) as err:
            print("No data returned because {0}".format(err))

    return df

def get_fundamental_data(mkt: int, group: int, date: datetime.date):
    """Historical Daily Data"""
    hist_conn = iq.HistoryConn(name="pyiqfeed-Example-daily-data")
    hist_listener = iq.VerboseIQFeedListener("History Bar Listener")
    hist_conn.add_listener(hist_listener)

    with iq.ConnConnector([hist_conn]) as connector:
        try:
            daily_data = hist_conn.request_fundamental_data(mkt,group,date)
            print(daily_data)
            #cols = ["Timestamp", "High", "Low", "Open", "Close", "Volume", "Open Interest"]
            df = pd.DataFrame.from_records(daily_data)
        except (iq.NoDataError, iq.UnauthorizedError) as err:
            print("No data returned because {0}".format(err))

    return df

if __name__ == '__main__':

    launch_service()

    df = get_daily_data_betweenDates("SPY",datetime.date(2022,1,1),datetime.date(2022,6,1))

    df = get_fundamental_data(1,6,datetime.date(2022,3,5))

    print(df)
