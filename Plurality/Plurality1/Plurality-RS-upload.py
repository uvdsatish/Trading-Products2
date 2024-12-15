# This script is create the plurality output and upload it in to the table and excel
# V2 - Inaddition to plurality flags for industry groups, we calculate C80, C90, C10, C20, TC, AvgRS

import pandas as pd
import psycopg2
from io import StringIO
import datetime
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


def get_rs_ig(conn, dte):
    cursor = conn.cursor()
    postgreSQL_select_Query = "select * from rs_industry_groups where date = %s"
    cursor.execute(postgreSQL_select_Query, (dte,))
    rs_ind_records = cursor.fetchall()

    rs_df = pd.DataFrame(rs_ind_records,
                         columns=['date', 'industry', 'ticker', 'rs1', 'rs2', 'rs3', 'rs4', 'rs'])

    return rs_df


def check_flag(tmp_df, prop, thres, dir):
    tot = len(tmp_df.index)
    if dir == "long":
        gt_df = tmp_df.loc[tmp_df['rs'] >= thres]
        gt = len(gt_df.index)
        rs_prop = (gt / tot) * 100
    else:
        lt_df = tmp_df.loc[tmp_df['rs'] <= thres]
        lt = len(lt_df.index)
        rs_prop = (lt / tot) * 100

    if rs_prop >= prop:
        return "Y"
    else:
        return "N"


def update_plurality_df(ind_groups, rss_df, fin_df):
    tmp_df = pd.DataFrame()
    for group in ind_groups:
        tmp_df = rss_df.loc[rss_df['industry'] == group]
        p5090_f = check_flag(tmp_df, 50, 90, "long")
        p4080_f = check_flag(tmp_df, 40, 80, "long")
        p5010_f = check_flag(tmp_df, 50, 10, "short")
        p4020_f = check_flag(tmp_df, 40, 20, "short")
        AvgRS = tmp_df['rs'].mean()
        c65 = len(tmp_df.loc[tmp_df['rs'] >= 65].index)
        c70 = len(tmp_df.loc[tmp_df['rs'] >= 70].index)
        c80 = len(tmp_df.loc[tmp_df['rs'] >= 80].index)
        c90 = len(tmp_df.loc[tmp_df['rs'] >= 90].index)
        c35 = len(tmp_df.loc[tmp_df['rs'] <= 35].index)
        c30 = len(tmp_df.loc[tmp_df['rs'] <= 30].index)
        c10 = len(tmp_df.loc[tmp_df['rs'] <= 10].index)
        c20 = len(tmp_df.loc[tmp_df['rs'] <= 20].index)
        totC = tmp_df.shape[0]

        fin_df.loc[fin_df['industry'] == group, 'p5090'] = p5090_f
        fin_df.loc[fin_df['industry'] == group, 'p4080'] = p4080_f
        fin_df.loc[fin_df['industry'] == group, 'p5010'] = p5010_f
        fin_df.loc[fin_df['industry'] == group, 'p4020'] = p4020_f
        fin_df.loc[fin_df['industry'] == group, 'AvgRS'] = round(AvgRS, 2)
        fin_df.loc[fin_df['industry'] == group, 'c80'] = c80
        fin_df.loc[fin_df['industry'] == group, 'c90'] = c90
        fin_df.loc[fin_df['industry'] == group, 'c10'] = c10
        fin_df.loc[fin_df['industry'] == group, 'c20'] = c20
        fin_df.loc[fin_df['industry'] == group, 'c65'] = c65
        fin_df.loc[fin_df['industry'] == group, 'c70'] = c70
        fin_df.loc[fin_df['industry'] == group, 'c35'] = c35
        fin_df.loc[fin_df['industry'] == group, 'c30'] = c30
        fin_df.loc[fin_df['industry'] == group, 'totC'] = totC

    return fin_df


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

def get_counts(rs_df,run_date):
    p5090 = get_plurality_filters(rs_df, "p5090")

    p4080 = rs_df.loc[rs_df['p4080'] == "Y"]
    p4080 = p4080.loc[p4080['p5090'] != "Y", "industry"]

    p5010 = get_plurality_filters(rs_df, "p5010")

    p4020 = rs_df.loc[rs_df['p4020'] == "Y"]
    p4020 = p4020.loc[p4020['p5010'] != "Y", "industry"]

    dct = {}
    dct['date'] = run_date
    dct['p5090c'] = len(p5090.index)
    dct['p4080c'] = len(p4080.index)
    dct['p5010c'] = len(p5010.index)
    dct['p4020c'] = len(p4020.index)
    dct['longTot'] = dct['p5090c'] + dct['p4080c']
    dct['shortTot'] = dct['p5010c'] + dct['p4020c']

    return dct

def get_plurality_filters(rs_df,plu_flag):
    plu_S = rs_df.loc[rs_df[plu_flag]=="Y",'industry']
    return pd.Series(plu_S)



if __name__ == '__main__':
    param_dic = {
        "host": "localhost",
        "database": "markets_technicals",
        "user": "postgres",
        "password": "root"
    }

    con = connect(param_dic)
    dateTimeObj = datetime.datetime.now()
    run_date1 = dateTimeObj - datetime.timedelta(days=0)
    run_date = run_date1.strftime("%Y-%m-%d")
    print(run_date)

    rss_df = get_rs_ig(con, run_date)
    ind_groups = list(rss_df.industry.unique())

    fin_df = pd.DataFrame()
    fin_df['industry'] = ind_groups
    fin_df['date'] = pd.Series([rss_df['date'][0] for x in range(len(fin_df.index))])

    fin_df = update_plurality_df(ind_groups, rss_df, fin_df)

    fin_df = fin_df[['date', 'industry', 'p5090', 'p4080', 'p5010', 'p4020', 'c80', 'c90', 'c10', 'c20', 'totC', 'AvgRS', 'c65',
         'c70', 'c35', 'c30']]

    update_ind_groups_plurality(con, fin_df, "rs_industry_groups_plurality")

    update_ind_groups_plurality(con, fin_df, "rs_industry_groups_plurality_history")

    cl_dct = get_counts(fin_df,run_date1)
    cl_dct = {k: [v] for k, v in cl_dct.items()}
    cl_df = pd.DataFrame.from_dict(cl_dct)

    update_ind_groups_plurality(con, cl_df, "rs_plurality_count_historical")

    con.close()

    sys.exit(0)
