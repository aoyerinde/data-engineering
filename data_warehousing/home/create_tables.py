import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    :param cur: cursor to execute the insert statements
    :param conn: connection string to postgres db
    result: executes the  drop tables queries
    """
    for query in drop_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()    

def create_tables(cur, conn):
    """
    :param cur: cursor to execute the insert statements
    :param conn: connection string to postgres db
    result: executes the  drop tables queries
    """
    for query in create_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()
        

def main():
    """
    main function to execute other methods
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    drop_tables(cur, conn)    
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()