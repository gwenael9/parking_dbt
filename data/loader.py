import duckdb

sql_import = '''
DROP VIEW IF EXISTS st_park_p;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE OR REPLACE TABLE staging.st_park_p AS 
SELECT * FROM read_csv_auto(
    'st_park_p.csv',
    normalize_names=True
)
'''

with duckdb.connect('database.db') as db:
    db.sql(sql_import)