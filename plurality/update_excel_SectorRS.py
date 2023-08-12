# This writes dataframe with sector plurality data to excel based on date

import pandas as pd
import psycopg2
import datetime

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
    postgreSQL_select_Query = "select * from rs_sectors_plurality where date = %s"
    cursor.execute(postgreSQL_select_Query,(dte,))
    rs_ind_records = cursor.fetchall()

    rs_df = pd.DataFrame(rs_ind_records,
                         columns=['date', 'sector', 'p5090', 'p4080', 'p5010', 'p4020','c80','c90','c10','c20','totc','avgrs'])

    return rs_df


def get_plurality_filters(rs_df,plu_flag):
    plu_S = rs_df.loc[rs_df[plu_flag]=="Y",'sector']
    return pd.Series(plu_S)


def get_plurality_df(rs_df):

    p5090 = get_plurality_filters(rs_df, "p5090")

    p4080 = rs_df.loc[rs_df['p4080'] == "Y"]
    p4080 = p4080.loc[p4080['p5090'] != "Y", "sector"]

    p5010 = get_plurality_filters(rs_df, "p5010")

    p4020 = rs_df.loc[rs_df['p4020'] == "Y"]
    p4020 = p4020.loc[p4020['p5010'] != "Y", "sector"]

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
    postgreSQL_select_Query = "select sector, ticker, rs from rs_sectors"
    cursor.execute(postgreSQL_select_Query)
    stock_records = cursor.fetchall()

    df = pd.DataFrame(stock_records,
                      columns=['sector','ticker', 'rs'])
    df = df[df['sector'].isin(t_g)]
    df.sort_values(by=['rs'],inplace=True,ascending=False)
    t_list = list(df.ticker.unique())
    num_l = int(round(0.1*len(t_list),0))
    return t_list[0:num_l]


def get_laggards(conn,b_g):
    cursor = conn.cursor()
    postgreSQL_select_Query = "select sector, ticker, rs from rs_sectors"
    cursor.execute(postgreSQL_select_Query)
    stock_records = cursor.fetchall()

    df = pd.DataFrame(stock_records,
                      columns=['sector','ticker', 'rs'])
    df = df[df['sector'].isin(b_g)]
    df.sort_values(by=['rs'], inplace=True)
    b_list = list(df.ticker.unique())
    num_b = int(round(0.1*len(b_list),0))
    return b_list[0:num_b]




if __name__ == '__main__':

    param_dic = {
        "host": "localhost",
        "database": "Plurality",
        "user": "postgres",
        "password": "root"
    }

    con = connect(param_dic)

    delta_days_from_current_date = 1
    dateTimeObj = datetime.datetime.now()
    run_date = dateTimeObj - datetime.timedelta(days=delta_days_from_current_date)
    run_date = run_date.strftime("%Y-%m-%d")

    print(run_date)

    rss_df = get_plurality_data(con, run_date)

    plu_df = get_plurality_df(rss_df)

    ll_df = get_leaders_laggards(con,plu_df)

    ll_df.to_excel(r"C:\Users\uvdsa\Documents\Trading\Scripts\plurality-output-sec.xlsx", index=False)

    con.close()
















