# This script creates sector and groups tables with top market cap stocks
import psycopg2

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



def create_tables(conn):
    commands = (
        """
        DROP TABLE IF EXISTS rs_sectors;
        """
       """
        DROP TABLE IF EXISTS rs_sectors_plurality;
        """ 
        """
        CREATE TABLE rs_sectors (
            date DATE NOT NULL,
            sector VARCHAR(255),
            ticker VARCHAR(255),
            RS1 FLOAT,
            RS2 FLOAT,
            RS3 FLOAT,
            RS4 FLOAT,
            RS FLOAT,
            PRIMARY KEY (date, sector, ticker)
            )          
        """,
        """
        CREATE TABLE rs_sectors_plurality(
            date DATE NOT NULL,
            sector VARCHAR(255),
            P5090 VARCHAR DEFAULT 'N',
            P4080 VARCHAR DEFAULT 'N',
            P5010 VARCHAR DEFAULT 'N',
            P4020 VARCHAR DEFAULT 'N',
            c80 FLOAT,
            c90 FLOAT,
            c10 FLOAT,
            c20 FLOAT,
            totC FLOAT,
            AvgRS FLOAT,
            PRIMARY KEY (date, sector) 
            )         
               """
    )

    try:
        cur = conn.cursor()
        for command in commands:
            cur.execute(command)
        cur.close()
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)



if __name__ == '__main__':

    param_dic = {
        "host": "localhost",
        "database": "Plurality",
        "user": "postgres",
        "password": "root"
    }

    con = connect(param_dic)

    create_tables(con)

    con.close()



