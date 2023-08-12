# delete header in  csv file
import pandas as pd

if __name__ == '__main__':

    file_name = r"D:\data\db\key_indicators_population_allTickers.csv"
    #csv to dataframe
    df = pd.read_csv(file_name)


   #replace empty string with 0
    df = df.fillna(0)

    #save to csv
    df.to_csv(file_name,header=False, index=False)





