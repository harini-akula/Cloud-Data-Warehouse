import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Description: This function can be used to execute queries used to load
    the staging tables in redshift database using COPY table queries.

    Arguments:
        cur: the cursor object. 
        conn: the connection object. 

    Returns:
        None. 
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Description: This function can be used to insert records into dimension 
    and fact tables in redshift database.

    Arguments:
        cur: the cursor object. 
        conn: the connection object. 

    Returns:
        None 
    """
    for query in insert_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def main():
    """
    Description: This function can be used to connect to Sparkify database and 
    execute queries used to load staging tables and insert records into 
    dimension and fact tables in redshift database.

    Arguments:
        cur: the cursor object. 
        conn: the connection object. 

    Returns:
        None. 
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(config.get('CLUSTER','host'), config.get('CLUSTER','db_name'), config.get('CLUSTER','db_user'), config.get('CLUSTER','db_password'), config.get('CLUSTER','db_port')))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()