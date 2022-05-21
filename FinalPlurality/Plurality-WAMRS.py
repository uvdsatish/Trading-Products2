# This is a script to do plurality based on WAM RS
import pandas as pd


def get_excel_data():
    df1 = pd.read_excel(r"C:\Users\uvdsa\Documents\Trading\Scripts\PluralityWAM.xlsx", header=None)
    df2 = pd.read_excel(r"C:\Users\uvdsa\Documents\Trading\Scripts\PluralityWAM.xlsx", sheet_name=1, header=None)
    i_df = pd.concat([df1, df2], axis=0)
    return i_df


def conv_dictionary(i_df):
    dct = {}
    kys = []
    values = []
    for row in i_df.itertuples(index=False, name=None):
        if row[1][0] == '~':
            if len(dct.keys()) == 0:
                kys.append(row[1][1:])
                dct[kys[0]] = None
            else:
                kys.append(row[1][1:])
                dct[list(dct)[-1]] = values
                values = []
                dct[kys[-1]] = None
        else:
            values.append(row)
    dct[list(dct)[-1]] = values
    return dct


def update_plurality(dct, p, t, d):
    lpt = []
    for k, v in dct.items():
        if check_sector(v, p, t, d):
            lpt.append(k)
    return lpt


def check_sector(list_tup, p1, t1, d1):
    tot_len = len(list_tup)
    count = 0
    for tup in list_tup:
        if (d1 == "long") and (tup[10] >= t1):
            count += 1
        if (d1 == "short") and (tup[10] <= t1):
            count += 1
    prop_check = count / tot_len
    if prop_check >= p1:
        return True
    else:
        return False


if __name__ == '__main__':
    ii_df = get_excel_data()
    c_dct = conv_dictionary(ii_df)
    prop = 0.4
    threshold = 20
    direction = "short"
    f_list = update_plurality(c_dct, prop, threshold, direction)
    print(len(f_list))
    print(f_list)
