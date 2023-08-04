""" This script uploads main lookup all in to database - all tickers, groups, sectors, market cap, may be this can be modified or used to update delta: compare two datasets, find the delta1 and delta2; delta1 marks inactive, delta2 inserts, and any thing that is not inactive is marked active"""
import pandas as pd
import psycopg2
import sqlalchemy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Date, BigInteger, Float
from sqlalchemy import create_engine
from io import StringIO

Base = declarative_base()


class iblkupall(Base):
    __tablename__ = 'iblkupall'
    id = Column(Integer, primary_key=True)
    industry = Column(String)
    ticker = Column(String)
    name = Column(String)
    sector = Column(String)
    volume = Column(BigInteger)
    marketcap = Column(Float)

    def __repr__(self):
        return "<Book(industry='{}', ticker='{}', name={}, sector={}, volume={}, marketcap={})>" \
            .format(self.industry, self.ticker, self.name, self.sector, self.volume, self.marketcap)

def recreate_database():
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

def read_data_fromIB():
    ibdf = pd.read_excel(r"C:\Users\uvdsa\Documents\Trading\Scripts\IndustryGroups-All.xlsx")
    return ibdf


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


def copy_from_stringio(conn, dff, table):
    """
    Here we are going save the dataframe in memory
    and use copy_from() to copy it to the table
    """
    # save dataframe to an in memory buffer
    buffer = StringIO()
    dff.to_csv(buffer, index_label='id', header=False)

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

    ibtot = read_data_fromIB()

    DATABASE_URI = 'postgresql+psycopg2://postgres:root@localhost:5432/Plurality'
    engine = create_engine(DATABASE_URI)

    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

    con = connect(param_dic)
    copy_from_stringio(con, ibtot, "iblkupall")
    con.close()

