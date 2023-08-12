# This script plots the graph of plurality count over days
""" get all plurality history
for each date: get the 4 counts: get dates list from prior step; unique and ordered list of dates: for each date get the sub-ddta frame and get the 4 counts and create a df
update a new history table with counts from the data frame and  park leaders and laggards count for now"""

import pandas as pd
import psycopg2
import sys
from io import StringIO


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

def get_all_plurality_data(conn):
    cursor = conn.cursor()
    postgreSQL_select_Query = "select * from rs_industry_groups_plurality_history"
    cursor.execute(postgreSQL_select_Query)
    rs_ind_records = cursor.fetchall()

    rs_df = pd.DataFrame(rs_ind_records,
                         columns=['date', 'industry', 'p5090', 'p4080', 'p5010', 'p4020','c80','c90','c10','c20','totc','avgrs','c65','c70','c35','c30'])

    return rs_df


def get_plurality_filters(rs_df,plu_flag):
    plu_S = rs_df.loc[rs_df[plu_flag]=="Y",'industry']
    return pd.Series(plu_S)


def get_plurality_dfcount(rs_df, date):

    p5090 = get_plurality_filters(rs_df, "p5090")

    p4080 = rs_df.loc[rs_df['p4080'] == "Y"]
    p4080 = p4080.loc[p4080['p5090'] != "Y", "industry"]

    p5010 = get_plurality_filters(rs_df, "p5010")

    p4020 = rs_df.loc[rs_df['p4020'] == "Y"]
    p4020 = p4020.loc[p4020['p5010'] != "Y", "industry"]

    dct = {}
    dct['date'] = date
    dct['p5090c'] = len(p5090.index)
    dct['p4080c'] = len(p4080.index)
    dct['p5010c'] = len(p5010.index)
    dct['p4020c'] = len(p4020.index)
    dct['longTot'] = dct['p5090c']+dct['p4080c']
    dct['shortTot'] = dct['p5010c']+dct['p4020c']


    return dct



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

def update_ind_groups_plurality(conn, dff, table):
    """
    Here we are going save the dataframe in memory
    and use copy_from() to copy it to the table
    """
    # save dataframe to an in memory buffer
    buffer = StringIO()
    dff.to_csv(buffer, index=False, header=False)

    buffer.seek(0)

    cursor = conn.cursor()
    try:
        cursor.copy_from(buffer, table, sep=",")
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:

        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("copy_from_stringio() done")
    cursor.close()




if __name__ == '__main__':

    param_dic = {
        "host": "localhost",
        "database": "Plurality",
        "user": "postgres",
        "password": "root"
    }

    con = connect(param_dic)

    rs_all_df = get_all_plurality_data(con)

    rs_dates_df = rs_all_df.loc[rs_all_df['industry'] == 'Energy-Coal', 'date']
    rs_dates_df.sort_values(inplace=True)

    rs_dates_list = list(rs_dates_df.unique())
    count_df_all = pd.DataFrame()

    for date in rs_dates_list:
        tmp_df = rs_all_df.loc[rs_all_df['date'] == date]
        dct_count= get_plurality_dfcount(tmp_df, date)
        dct_count = {k: [v] for k, v in dct_count.items()}
        count_df = pd.DataFrame.from_dict(dct_count)
        count_df_all = pd.concat([count_df_all,count_df])

    update_ind_groups_plurality(con, count_df_all, "rs_plurality_count_historical")


    con.close()
















