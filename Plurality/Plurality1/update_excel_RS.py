# This writes dataframe with plurality data to excel based on date

import pandas as pd
import psycopg2
import datetime
import os
import sys

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

def get_plurality_data(conn,dte):
    cursor = conn.cursor()
    postgreSQL_select_Query = "select * from rs_industry_groups_plurality where date = %s"
    cursor.execute(postgreSQL_select_Query,(dte,))
    rs_ind_records = cursor.fetchall()

    rs_df = pd.DataFrame(rs_ind_records,
                         columns=['date', 'industry', 'p5090', 'p4080', 'p5010', 'p4020','c80','c90','c10','c20','totc','avgrs','c65','c70','c35','c30'])

    return rs_df


def get_plurality_filters(rs_df,plu_flag):
    plu_S = rs_df.loc[rs_df[plu_flag]=="Y",'industry']
    return pd.Series(plu_S)


def get_plurality_df(rs_df):

    p5090 = get_plurality_filters(rs_df, "p5090")

    p4080 = rs_df.loc[rs_df['p4080'] == "Y"]
    p4080 = p4080.loc[p4080['p5090'] != "Y", "industry"]

    p5010 = get_plurality_filters(rs_df, "p5010")

    p4020 = rs_df.loc[rs_df['p4020'] == "Y"]
    p4020 = p4020.loc[p4020['p5010'] != "Y", "industry"]

    dct = {}
    dct['p5090'] = len(p5090.index)
    dct['p4080'] = len(p4080.index)
    dct['p5010'] = len(p5010.index)
    dct['p4020'] = len(p4020.index)

    max_plural = max(dct, key=dct.get)

    agg_df = pd.concat([p5090, p4080, p5010, p4020], axis=1, ignore_index=True)
    col_names = ['p5090', 'p4080', 'p5010', 'p4020']
    agg_df.columns = col_names
    agg_df.fillna(value="", axis=0, inplace=True)

    list1 = lista = listb = listc = listd = []
    fin_df = pd.DataFrame()

    list1 = agg_df[max_plural].to_list()
    list1 = [x for x in list1 if x != ""]
    fin_df[max_plural] = pd.Series(list1)

    if max_plural != 'p5090':
        lista = agg_df['p5090'].to_list()
        lista = [x for x in lista if x != ""]
        fin_df['p5090'] = pd.Series(lista)

    if max_plural != 'p4080':
        listb = agg_df['p4080'].to_list()
        listb = [x for x in listb if x != ""]
        fin_df['p4080'] = pd.Series(listb)

    if max_plural != 'p5010':
        listc = agg_df['p5010'].to_list()
        listc = [x for x in listc if x != ""]
        fin_df['p5010'] = pd.Series(listc)

    if max_plural != 'p4020':
        listd = agg_df['p4020'].to_list()
        listd = [x for x in listd if x != ""]
        fin_df['p4020'] = pd.Series(listd)

    fin_df.fillna("", inplace=True)

    fin_df = fin_df.reindex(columns=['p5090', 'p4080', 'p5010', 'p4020'])

    return fin_df

def get_leaders_laggards(con,fin_df):
    top_groups = list(fin_df['p5090'])
    bottom_groups = list(fin_df['p5010'])

    leaders_list = get_leaders(con,top_groups)
    laggards_list = get_laggards(con,bottom_groups)

    fin_df['leaders'] = pd.Series(leaders_list)
    fin_df['laggards'] = pd.Series(laggards_list)

    fin_df.fillna("", inplace=True)

    fin_df = fin_df.reindex(columns=['p5090', 'p4080', 'p5010', 'p4020', 'leaders', 'laggards'])

    return fin_df

def get_leaders(conn,t_g):
    cursor = conn.cursor()
    postgreSQL_select_Query = "select industry, ticker, rs from rs_industry_groups"
    cursor.execute(postgreSQL_select_Query)
    stock_records = cursor.fetchall()

    df = pd.DataFrame(stock_records,
                      columns=['industry','ticker', 'rs'])
    df = df[df['industry'].isin(t_g)]
    df.sort_values(by=['rs'],inplace=True,ascending=False)
    t_list = list(df.ticker.unique())
    num_l = int(round(0.1*len(t_list),0))
    return t_list[0:num_l]


def get_laggards(conn,b_g):
    cursor = conn.cursor()
    postgreSQL_select_Query = "select industry, ticker, rs from rs_industry_groups"
    cursor.execute(postgreSQL_select_Query)
    stock_records = cursor.fetchall()

    df = pd.DataFrame(stock_records,
                      columns=['industry','ticker', 'rs'])
    df = df[df['industry'].isin(b_g)]
    df.sort_values(by=['rs'], inplace=True)
    b_list = list(df.ticker.unique())
    num_b = int(round(0.1*len(b_list),0))
    return b_list[0:num_b]

def get_top_groups(rs_df):

    top_df = rs_df.loc[(rs_df['totc'] >= 6) & (rs_df['avgrs'] >=80)]
    top_df = top_df[['industry','totc','avgrs']]

    return top_df


def get_bottom_groups(rs_df):
    bot_df = rs_df.loc[(rs_df['totc'] >= 6) & (rs_df['avgrs'] <= 20)]
    bot_df = bot_df[['industry', 'totc', 'avgrs']]

    return bot_df

def merge_for_groupDf(top_df, bot_df ):
    top_groups = list(top_df['industry'])
    top_groups_count = list(top_df['totc'])
    top_groups_avgRS = list(top_df['avgrs'])

    bottom_groups = list(bot_df['industry'])
    bottom_groups_count = list(bot_df['totc'])
    bottom_groups_avgRS = list(bot_df['avgrs'])

    tb_df = pd.DataFrame()

    tb_df['topGroups'] = pd.Series(top_groups)
    tb_df['topGroupCount'] = pd.Series(top_groups_count)
    tb_df['topGroupAvgRS'] = pd.Series(top_groups_avgRS)

    tb_df['BottomGroups'] = pd.Series(bottom_groups)
    tb_df['BottomGroupCount'] = pd.Series(bottom_groups_count)
    tb_df['BottomGroupAvgRS'] = pd.Series(bottom_groups_avgRS)

    tb_df.fillna("", inplace=True)

    tb_df = tb_df.reindex(columns=['topGroups', 'topGroupCount', 'topGroupAvgRS', 'BottomGroups', 'BottomGroupCount', 'BottomGroupAvgRS'])

    return tb_df

if __name__ == '__main__':

    param_dic = {
        "host": "localhost",
        "database": "markets_technicals",
        "user": "postgres",
        "password": "root"
    }

    con = connect(param_dic)

    delta_days_from_current_date = 0
    dateTimeObj = datetime.datetime.now()
    run_date2 = dateTimeObj - datetime.timedelta(days=delta_days_from_current_date)
    run_date = run_date2.strftime("%Y-%m-%d")

    print(run_date)

    rss_df = get_plurality_data(con, run_date)

    plu_df = get_plurality_df(rss_df)

    ll_df = get_leaders_laggards(con,plu_df)

    topGroups_df = get_top_groups(rss_df)
    bottomGroups_df = get_bottom_groups(rss_df)

    group_df = merge_for_groupDf(topGroups_df,bottomGroups_df)

    final_df = pd.concat([ll_df,group_df], axis="columns")


    path_str = r"D:\Trading Dropbox\Satish Udayagiri\SatishUdayagiri\Trading\Plurality\Plurality1"

    if len(str(run_date2.month)) == 1:
        mth_str = "0" + str(run_date2.month)
    else:
        mth_str = str(run_date2.month)

    if len(str(run_date2.day)) == 1:
        day_str = "0" + str(run_date2.day)
    else:
        day_str = str(run_date2.day)

    dir_str = "D" + str(run_date2.year) + mth_str + day_str

    pth = os.path.join(path_str, dir_str)


    if not os.path.exists(pth):
        os.mkdir(pth)

    file_path = os.path.join(pth,"plurality-output.xlsx")


    final_df.to_excel(file_path, index=False)

    con.close()

    sys.exit(0)
















