import duckdb

# Connect to DuckDB. If 'my_duckdb.duckdb' does not exist, it will be created.
con = duckdb.connect('package/database/bums_and_roses.duckdb')

sqls=['rolling-dates', 'legend-map', 'joins-output', 'final-output', 'prod-var-prop']
for sql in sqls:
    with open(f'package/sql/{sql}.sql') as f:
        query = f.read()
        con.execute(query)
