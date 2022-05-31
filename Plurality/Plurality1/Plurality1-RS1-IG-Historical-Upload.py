#This script is create the plurality output historically and upload it in to the table and excel
#V2 - Inaddition to plurality flags for industry groups, we calculate C80, C90, C10, C20, TC, AvgRS
# get dates from industry_groups

import pandas as pd
import psycopg2
from io import StringIO
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


def get_distinct_ig(conn):
    cursor = conn.cursor()
    postgreSQL_select_Query = "select distinct(industry) from industry_groups"
    cursor.execute(postgreSQL_select_Query)
    rs_ind_records = cursor.fetchall()

    rs_df = pd.DataFrame(rs_ind_records,
                      columns=['industry'])
    ig_list = rs_df.to_list()

    return ig_list


def get_all_rs_ig(conn):
    cursor = conn.cursor()
    postgreSQL_select_Query = "select * from rs_industry_groups_history"
    cursor.execute(postgreSQL_select_Query)
    rs_ind_records = cursor.fetchall()

    rs_df = pd.DataFrame(rs_ind_records,
                      columns=['date','industry', 'ticker', 'rs1', 'rs2', 'rs3', 'rs4', 'rs'])

    return rs_df


def get_rs_dates(conn):
    cursor = conn.cursor()
    postgreSQL_select_Query = "select distinct(date) from rs_industry_groups_history"
    cursor.execute(postgreSQL_select_Query)
    rs_ind_records = cursor.fetchall()

    rs_df = pd.DataFrame(rs_ind_records,
                         columns=['date'])

    dates_str = rs_df.to_list()

    return dates_str


yu

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
        fin_df.loc[fin_df['industry'] == group, 'AvgRS'] = round(AvgRS,2)
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


def update_historical_plurality(rss_df,date_str,ind_groups):
    big_df = pd.DataFrame()

    for dte in date_str:
        tmp_df = pd.DataFrame()
        tmp_df['industry'] = ind_groups
        tmp_df['date'] = pd.Series([dte for x in range(len(tmo_df.index))])

        tmp_df = update_plurality_df(ind_groups, rss_df, tmp_df)
        tmp_df = tmp_df[
            ['date', 'industry', 'p5090', 'p4080', 'p5010', 'p4020', 'c80', 'c90', 'c10', 'c20', 'totC', 'AvgRS', 'c65',
             'c70', 'c35', 'c30']]
        big_df = pd.concat([big_df,tmp_df])

    return big_df




if __name__ == '__main__':

    param_dic = {
        "host": "localhost",
        "database": "Plurality",
        "user": "postgres",
        "password": "root"
    }

    con = connect(param_dic)

    ind_groups = get_distinct_ig(con)
    rss_df = get_all_rs_ig(con)
    date_str = get_rs_dates(con)

    his_df=update_historical_plurality(rss_df,date_str,ind_groups)


    update_ind_groups_plurality(con,his_df,"rs_industry_groups_plurality_history")

    con.close()







