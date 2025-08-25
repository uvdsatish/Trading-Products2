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

def get_count_data(conn,date):
    cursor = conn.cursor()
    postgreSQL_select_Query = """select * from rs_plurality_count_historical where date <= %s"""
    cursor.execute(postgreSQL_select_Query, [date, ])
    plot_data = cursor.fetchall()
    p_f = pd.DataFrame(plot_data, columns=['date', 'p5090c', 'p4080c', 'p5010c', 'p4020c', 'longtot', 'shorttot'])
    p_f.sort_values(by='date', inplace=True)

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

def store_plots(lists_dct,pl_df,c_df,dte,pth):
    make_directories(pth,dte)
    plot_count(c_df,dte,pth)
    for key,value in lists_dct.items():
        print("storing the plots for %s" % key)
        if key == 'top_long':
            dir = "long"
            cat = "top-long"
        elif key == 'long_watch':
            dir ="long"
            cat ="watch-long"
        elif key == 'top_short':
            dir = "short"
            cat = "top-short"
        elif key == 'short_watch':
            dir = "short"
            cat = "watch-short"
        else:
            dir = "tmpD"
            cat = "tmpC"

        for v in value:
            print("storing the plots for item %s" % v)
            tmp_df = pl_df.loc[pl_df['industry'] == v]
            plot_store_rs(tmp_df,dte,pth,dir,cat,v)

    return


def plot_store_rs(tmp_df,dte,pth,dir,cat,v):

    dte = dte - datetime.timedelta(days=1)

    if len(str(dte.month)) == 1:
        mth_str = "0"+str(dte.month)
    else:
        mth_str = str(dte.month)

    if len(str(dte.day)) == 1:
        day_str = "0"+str(dte.day)
    else:
        day_str = str(dte.day)


    dir_str = "D" + str(dte.year) + mth_str + day_str

    v_str = v+".jpg"
    v_str = re.sub('[/]+', '-', v)

    title ="relative strength for %s" %v


    file_path = os.path.join(pth, dir_str,dir,cat,v_str)

    sns_plot=sns.relplot( data = tmp_df, x ='date', y ='avgrs', height=10, aspect=2.4, kind="line",  )
    sns_plot.fig.subplots_adjust(top=.9)
    sns_plot.fig.suptitle(title)

    plt.axhline(y=70, color='g',linestyle='dotted')
    plt.axhline(y=30, color='r', linestyle='dotted')


    sns_plot.figure.savefig(file_path)

    plt.clf()

    return

def plot_count(tmp_df,dte,pth):

    dte = dte - datetime.timedelta(days=1)

    if len(str(dte.month)) == 1:
        mth_str = "0"+str(dte.month)
    else:
        mth_str = str(dte.month)

    if len(str(dte.day)) == 1:
        day_str = "0"+str(dte.day)
    else:
        day_str = str(dte.day)


    dir_str = "D" + str(dte.year) + mth_str + day_str

    title ="Plurality_count plot"


    file_path = os.path.join(pth, dir_str,"pl_count.jpg")

    tmp_df = tmp_df[['date', 'longtot', 'shorttot']]
    tmp_df=tmp_df.drop_duplicates(subset=['date'], keep='last')

    sns_plot = sns.relplot(x='date',y= 'value', hue='variable', data=pd.melt(tmp_df, 'date'), kind='line', height=10,
                           aspect=2.4, palette="cool")
    sns_plot.fig.subplots_adjust(top=.9)
    sns_plot.fig.suptitle(title)


    sns_plot.figure.savefig(file_path)

    plt.clf()

    return


def make_directories(pth,dte):
    dte = dte - datetime.timedelta(days=1)


    if len(str(dte.month)) == 1:
        mth_str = "0"+str(dte.month)
    else:
        mth_str = str(dte.month)

    if len(str(dte.day)) == 1:
        day_str = "0"+str(dte.day)
    else:
        day_str = str(dte.day)


    dir_str = "D" + str(dte.year) + mth_str + day_str
    print(dir_str)

    dir = os.path.join(pth,dir_str)
    print(dir)
    if not os.path.exists(dir):
        os.mkdir(dir)

    pth1 = os.path.join(dir,"long")
    if not os.path.exists(pth1):
        os.mkdir(pth1)
    pth2 = os.path.join(dir, "short")
    if not os.path.exists(pth2):
        os.mkdir(pth2)

    pthA = os.path.join(pth1, "top-long")
    if not os.path.exists(pthA):
        os.mkdir(pthA)
    pthB = os.path.join(pth1, "watch-long")
    if not os.path.exists(pthB):
        os.mkdir(pthB)

    pthC = os.path.join(pth2, "top-short")
    if not os.path.exists(pthC):
        os.mkdir(pthC)
    pthD = os.path.join(pth2, "watch-short")
    if not os.path.exists(pthD):
        os.mkdir(pthD)


def export_rs_plurality_to_csv_copy(connection, output_path):
    """
    Export rs_plurality_count_historical PostgreSQL table to CSV using COPY command.
    Will overwrite the file if it already exists.

    Parameters:
    -----------
    connection : psycopg2.extensions.connection
        PostgreSQL database connection
    output_path : str
        The path where the CSV file should be saved

    Returns:
    --------
    str
        The path where the CSV file was saved
    """
    try:
        import os

        # Ensure the directory exists
        os.makedirs(os.path.dirname(output_path) if os.path.dirname(output_path) else '.', exist_ok=True)

        # Create a cursor
        cursor = connection.cursor()

        # Use COPY command to export data - 'w' mode will overwrite existing file
        with open(output_path, 'w', newline='') as f:
            # Get column names first
            cursor.execute("""
                SELECT array_to_string(
                    array_agg(column_name ORDER BY ordinal_position),
                    ','
                )
                FROM information_schema.columns
                WHERE table_name = 'rs_plurality_count_historical';
            """)
            headers = cursor.fetchone()[0]
            f.write(headers + '\n')

            # Copy data
            cursor.copy_expert(
                "COPY rs_plurality_count_historical TO STDOUT WITH CSV",
                f
            )

        print(f"Data successfully exported to: {output_path}")
        return output_path

    except Exception as e:
        print(f"Error exporting data: {str(e)}")
        raise
    finally:
        if cursor:
            cursor.close()


if __name__ == '__main__':

    param_dic = {
        "host": "localhost",
        "database": "markets_technicals",
        "user": "postgres",
        "password": "root"

    }


    pth = r"D:\Trading Dropbox\Satish Udayagiri\SatishUdayagiri\Trading\Plurality\Plurality1"
    con = connect(param_dic)
    dateTimeObj = datetime.datetime.now()
    date1 = dateTimeObj - datetime.timedelta(days=-1) ## one less day
    datee = dateTimeObj - datetime.timedelta(days=0) ##the day in focus

    print("first date %s" % datee)

    if len(str(datee.month)) == 1:
        mth_str = "0" + str(datee.month)
    else:
        mth_str = str(datee.month)

    if len(str(datee.day)) == 1:
        day_str = "0" + str(datee.day)
    else:
        day_str = str(datee.day)

    dir_str = "D" + str(datee.year) + mth_str + day_str

    pth1 = os.path.join(pth, dir_str)

    plu_excel = os.path.join(pth1, "plurality-output.xlsx")

    df = get_plot_data(con,date1)
    lists_dct = get_lists(plu_excel)

    c_df = get_count_data(con, date1)

    cl_df = c_df[['date','p5090c','p5010c','longtot','shorttot']]

    store_plots(lists_dct,df,cl_df,date1,pth)


    output_path = "C:/Users/uvdsa/OneDrive/Desktop/rs_plurality_data.csv"
    export_rs_plurality_to_csv_copy(con, output_path)


    con.close()

    sys.exit(0)


