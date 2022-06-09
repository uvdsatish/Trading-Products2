# Draw annual plots for specified groups for a particular date

import pandas as pd
import matplotlib.pyplot as plt
import os
import sys
import datetime
import psycopg2
import seaborn as sns
import regex as re

sns.set(style="darkgrid")

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


def get_plot_data(conn, date):
    cursor = conn.cursor()
    postgreSQL_select_Query = """select * from rs_industry_groups_plurality_history where date <= %s"""
    cursor.execute(postgreSQL_select_Query, [date, ])
    plot_data = cursor.fetchall()
    p_f = pd.DataFrame(plot_data, columns=['date', 'industry', 'p5090', 'p4080', 'p5010', 'p4020', 'c80','c90', 'c10', 'c20', 'totc', 'avgrs', 'c65', 'c70', 'c35', 'c30'])
    p_f['c65per'] = round((p_f['c65']/p_f['totc'])*100,0)
    p_f['c70per'] = round((p_f['c70'] / p_f['totc'])*100,0)

    p_f.sort_values(by='date', ascending=False, inplace=True)


    return p_f

def get_lists(excl_file):
    plu_dct ={}
    plu_df = pd.read_excel(excl_file)
    plu_dct['top_long'] = list(plu_df.loc[plu_df['p5090'].notnull(),'p5090'])
    plu_dct['long_watch'] = list(plu_df.loc[plu_df['p4080'].notnull(),'p4080'])
    plu_dct['top_short'] = list(plu_df.loc[plu_df['p5010'].notnull(),'p5010'])
    plu_dct['short_watch'] = list(plu_df.loc[plu_df['p4020'].notnull(),'p4020'])
    #plu_dct['leaders'] = list(plu_df.loc[plu_df['leaders'].notnull(),'leaders'])
    #plu_dct['laggards'] = list(plu_df.loc[plu_df['laggards'].notnull(),'laggards'])

    return plu_dct

def store_plots(lists,pl_df,dte,pth):
    make_directories(pth,dte)

    for ig in lists:
        print("storing the plots for item %s" % ig)
        tmp_df = pl_df.loc[pl_df['industry'] == ig]
        plot_store_rs(tmp_df,dte,pth,ig)

    return


def plot_store_rs(tmp_df,dte,pth,v):

    dte = dte - datetime.timedelta(days=0)

    if len(str(dte.month)) == 1:
        mth_str = "0" + str(dte.month)
    else:
        mth_str = str(dte.month)

    if len(str(dte.day)) == 1:
        day_str = "0" + str(dte.day)
    else:
        day_str = str(dte.day)

    dir_str = "D" + str(dte.year) + mth_str + day_str

    v_str = v+".jpg"
    v_str= re.sub('[/]+', '-', v)

    title ="relative strength for %s" %v


    file_path = os.path.join(pth, dir_str,"interestingChanges",v_str)

    sns_plot=sns.relplot( data = tmp_df, x ='date', y ='avgrs', height=10, aspect=2.4, kind="line", palette="cool" )
    sns_plot.fig.subplots_adjust(top=.9)
    sns_plot.fig.suptitle(title)

    plt.axhline(y=80, color='g',linestyle='dotted')
    plt.axhline(y=20, color='r', linestyle='dotted')


    sns_plot.figure.savefig(file_path)

    plt.clf()

    return


def make_directories(pth,dte):
    dte = dte - datetime.timedelta(days=0)

    if len(str(dte.month)) == 1:
        mth_str = "0"+str(dte.month)
    else:
        mth_str = str(dte.month)

    if len(str(dte.day)) == 1:
        day_str = "0"+str(dte.day)
    else:
        day_str = str(dte.day)


    dir_str = "D" + str(dte.year) + mth_str + day_str

    dir = os.path.join(pth,dir_str)
    if not os.path.exists(dir):
        os.mkdir(dir)

    pth1 = os.path.join(dir,"interestingChanges")
    if not os.path.exists(pth1):
        os.mkdir(pth1)




if __name__ == '__main__':

    param_dic = {
        "host": "localhost",
        "database": "Plurality",
        "user": "postgres",
        "password": "root"
    }

    #plu_excel = r"C:\Users\uvdsa\Documents\Trading\Scripts\plurality-output.xlsx"
    pth = r"D:\Trading Dropbox\Satish Udayagiri\SatishUdayagiri\Trading\Plurality\Plurality1"

    con = connect(param_dic)
    dateTimeObj = datetime.datetime.now()
    datee = dateTimeObj - datetime.timedelta(days=0)
    print("first date %s" % datee)

    df = get_plot_data(con,datee)
    #lists_dct = get_lists(plu_excel)
    groups_list = ['Transportation-Ship']

    store_plots(groups_list,df,datee, pth)


    con.close()


