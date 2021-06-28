import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, MetaData, Table, select
import pandas as pd

# Postgres DB-specific credentials and output path to be defined in local dotenv file
load_dotenv()

pg_username = os.environ.get('PG_USERNAME')
pg_password = os.environ.get('PG_PASSWORD')
pg_db = os.environ.get('PG_DATABASE')
csv_path = os.environ.get('RESULT_PATH')


def connect_to_relation(table_name):
    """
    Attempts to create a connection to database and returns sqlalchemy Table object

    Parameters
    ----------
    table_name: str
        Name of existing database relation for which to return Table object
    Returns
    -------
    sqlalchemy engine.base.Connection and Table object corresponding to identified postgres relation
    """
    engine = create_engine(f'postgresql://{pg_username}:{pg_password}@localhost:5432/{pg_db}')
    connection = engine.connect()
    meta = MetaData()
    meta.reflect(bind=engine)
    return (connection, Table(table_name, meta, autoload=True, autoload_with=engine))


def execute_query(connection, table):
    """
    Takes sqlalchemy Connection and Table objects and executes a select statement, saving results to a csv

    Parameters
    ----------
    connection: sqlalchemy.engine.base.Connection
    table: sqlalchemy.sql.schema.Table
        sqlalchemy Table object to query
    Returns
    -------
    None
    """
    stmt = select([table])
    results = connection.execute(stmt).fetchall()
    
    kdn_articles_df = pd.DataFrame(
        results, 
        columns=['id', 'title', 'date_published', 'author', 'author_info', 
            'tags', 'excerpt', 'post_text', 'url'])
    kdn_articles_df.to_csv(csv_path, 
        sep=',', header=table.columns,
        index=False, encoding='utf-8')


if __name__ == '__main__':
    conn, kdn_articles_table = connect_to_relation(table_name='kdn_articles')
    execute_query(conn, kdn_articles_table)