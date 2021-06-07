from commons import *
import pandas as pd
import sqlite3

con = sqlite3.connect('example.db')
cur = con.cursor()

# Create table
cur.execute('''CREATE TABLE IF NOT EXISTS kpis (category TEXT NOT NULL,
    kpi TEXT NOT NULL,
    date TEXT NOT NULL,
    value REAL,
    PRIMARY KEY(kpi,date)
); ''')


# Insert a row of data
cur.execute("REPLACE INTO kpis VALUES ('cat1', 'kpi1', '2020-01-01', 300)")
cur.execute("REPLACE INTO kpis VALUES ('cat1', 'kpi1', '2020-01-01', 400)")
cur.execute("REPLACE INTO kpis VALUES ('cat1', 'kpi2', '2020-01-01', 300)")

# Save (commit) the changes
con.commit()

for row in cur.execute('SELECT * FROM kpis'):
        print(row)

# We can also close the connection if we are done with it.
# Just be sure any changes have been committed or they will be lost.
con.close()
