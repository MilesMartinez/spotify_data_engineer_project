import sqlite3
from sql_queries import create_table_queries, drop_table_queries, create_view_queries, drop_view_queries

def drop_tables(cur, conn):
    """
    Description:
    Drops each table using the queries in `drop_table_queries` list.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

def create_tables(cur, conn):
    """
    Description:
    Creates each table using the queries in `create_table_queries` list. 
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

def drop_views(cur, conn):
    """
    Description:
    Drops each view using the queries in `drop_view_queries` list.
    """
    for query in drop_view_queries:
        cur.execute(query)
        conn.commit()


def create_views(cur, conn):
    """
    Description:
    Creates each view using the queries in `create_view_queries` list. 
    """
    for query in create_view_queries:
        cur.execute(query)
        conn.commit()


def main():
    
    conn = sqlite3.connect('spotify.db')
    cur = conn.cursor()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)
    drop_views(cur, conn)
    create_views(cur, conn)

    conn.close()

if __name__ == "__main__":
    main()