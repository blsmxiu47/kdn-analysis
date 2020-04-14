# pull_kdn_data.py
# Using sqlalchemy extract (all) data in from postgresql data table
#  'kdn_articles' and export to a format that we can use for cleaning
#  demos in python/pandas
from sqlalchemy import create_engine, MetaData, Table, select
import pandas as pd


engine = create_engine('postgresql://weswa:PASSWORD@localhost:5432/weswa')
connection = engine.connect()
meta = MetaData()
meta.reflect(bind=engine)

kdn_articles = Table('kdn_articles', meta, autoload=True, autoload_with=engine)

stmt = select([kdn_articles])
results = connection.execute(stmt).fetchall()
kdn_articles_df = pd.DataFrame(
    results, 
    columns=['id', 'title', 'date_published', 'author', 'author_info', 
        'tags', 'excerpt', 'post_text', 'url']
)

#kdn_articles_df.head(10)

kdn_articles_df.to_csv('C:\\Users\\weswa\\psql_pulls\\kdn_test_pull.csv', 
    sep=',', header=kdn_articles.columns,
    index=False, encoding='utf-8')