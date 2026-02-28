import duckdb

con = duckdb.connect('warehouse.db')

# 1. Stock entering the warehouse
con.execute("""
    CREATE TABLE IF NOT EXISTS stock_logs AS SELECT * FROM (VALUES 
        (101, '2026-02-01', 50), -- Product A arrives
        (101, '2026-02-15', 20)  -- More Product A arrives
    ) AS t(pid, date, qty_in)
""")

# 2. Sales records
con.execute("""
    CREATE TABLE IF NOT EXISTS sales_logs AS SELECT * FROM (VALUES 
        (101, '2026-02-05', 10), -- Normal Sale
        (101, '2026-02-10', 45), -- ERROR: Cumulative sales (55) > Cumulative stock (50)
        (102, '2026-02-12', 5)   -- ERROR: Sold Product B, but it was never stocked!
    ) AS t(pid, date, qty_out)
""")