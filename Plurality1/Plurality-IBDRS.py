# This is a script to do plurality based on IBD RS
#checking one more time

import pandas as pd


def get_excel_data():
    df = pd.read_excel(r"C:\Users\uvdsa\Documents\Trading\Scripts\PluralityIB.xlsx")
    df.drop(df.columns[3:], axis=1, inplace=True)
    df = df[["Industry Name", "Symbol", "RS Rating"]]
    df["Industry Name"] = df["Industry Name"].str.strip()
    df["Symbol"] = df["Symbol"].str.strip()
    df.sort_values(by=["Industry Name"], inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df


def conv_dictionary(i_df):
    dct = {}
    kys = []
    values = []
    temp = ""
    for row in i_df.itertuples(index=False, name=None):
        if row[0] != temp:
            if len(dct.keys()) == 0:
                kys.append(row[0])
                values.append([row[1], row[2]])
                temp = row[0]
                dct[kys[0]] = None
            else:
                kys.append(row[0])
                dct[list(dct)[-1]] = values
                values = []
                values.append([row[1], row[2]])
                dct[kys[-1]] = None
                temp = row[0]
        else:
            values.append([row[1], row[2]])
            temp = row[0]
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
        if (d1 == "long") and (tup[1] >= t1):
            count += 1
        if (d1 == "short") and (tup[1] <= t1):
            count += 1
    prop_check = count / tot_len
    if prop_check >= p1:
        return True
    else:
        return False


if __name__ == '__main__':
    ii_df = get_excel_data()
    c_dct = conv_dictionary(ii_df)
    print(c_dct)
    prop = 0.4
    threshold = 20
    direction = "short"
    f_list = update_plurality(c_dct, prop, threshold, direction)
    print(len(f_list))
    print(f_list)
