import pandas as pd
import pandera.pandas as pa
from pandera.pandas import Check, Column
import duckdb

## ====================================================================== ##
## PREPARATION: DB CONNECTION AND SCHEMAS                                 ##
## ====================================================================== ##
conn = duckdb.connect('warehouse.db')

stock_schema = pa.DataFrameSchema({

    # Multiple rules for 'pid'
    # 1. must be integer and not Null 
    # 2. greater than 100 because id starts from 101
    "pid": Column(
        pa.Int32, 
        Check.greater_than(100),
        nullable=False
    ),

    # Multiple rules for 'date': 
    # 1. must be able to be changed to datetime format 
    # 2. Not Null and String
    "date": Column(
        pa.DateTime,
        nullable=False
    ),

    # Multiple rules for 'qty_in': 
    # 1. must be integer, qty > 0, and not Null
    # 2. qty < 200 as the  max capacity for each item is 200
    "qty_in": Column(
        pa.Int32, 
        checks=[
            Check.greater_than(0), 
            Check.less_than(200),
        ],
        nullable=False
    )
})

sales_schema = pa.DataFrameSchema({

    # Multiple rules for 'pid'
    # 1. must be integer and not Null 
    # 2. greater than 100 because id starts from 101
    "pid": Column(
        pa.Int32, 
        Check.greater_than(100),
        nullable=False
    ),

    # Multiple rules for 'date': 
    # 1. must be able to be changed to datetime format 
    # 2. Not Null and String
    "date": Column(
        pa.DateTime,
        nullable=False
    ),

    # Multiple rules for 'qty_out': 
    # 1. must be integer, qty > 0, and not Null
    # 2. qty < 200 as the  max capacity for each item is 200
    "qty_out": Column(
        pa.Int32, 
        checks=[
            Check.greater_than(0), 
            Check.less_than(200),
        ],
        nullable=False
    )
})

## ====================================================================== ##
## PREPARATION: QUERYING                                                  ##
## ====================================================================== ##
query_sequence = {

    # Query for stock_logs
    "stock_audit": """
                   SELECT * FROM stock_logs
                   """,

    # Query for sales_logs
    "sales_audit": """
                   SELECT * FROM sales_logs
                   """, 

    # Query for setting the data to be prepared for
    # 1. Negative stock detection
    # 2. Never stocked product                                  
    "item_audit":  """
                   WITH tab1 AS (
                        SELECT
                            pid,
                            date,
                            qty_in AS qty
                        FROM stock_logs
                        UNION ALL
                        SELECT
                            pid,
                            date,
                            (qty_out * -1) AS qty
                        FROM sales_logs
                        ORDER BY 
                            pid, 
                            date
                   ),

                   tab2 AS (
                        SELECT 
                            pid,
                            date,
                            qty,
                            SUM(qty) OVER(
                                PARTITION BY pid
                                ORDER BY date)
                            AS qty_count
                        FROM tab1
                   )
                   
                   SELECT * FROM tab2
                   """
}

## ====================================================================== ##
## PHASE 1 OF AUDIT: STOCK AND SALES DATA INTEGRITY                       ##
## ====================================================================== ##
stock_df = conn.execute(query_sequence["stock_audit"]).df()

try:

    # This one line runs ALL your business rules at once
    stock_schema.validate(stock_df, lazy=True)
    print("✅ Data Quality Check Passed: Data is safe for AI/Production.")
    
except pa.errors.SchemaErrors as err:

    # 3. GENERATE THE "ERROR REPORT" (This is your value-add)
    print("❌ Data Quality Check Failed!")
    # This returns a dataframe of exactly which rows/cells broke which rules
    report = err.failure_cases 
    report.to_csv("stock_data_health_issues.csv")
    print("Detailed report saved to 'data_health_issues.csv'")

sales_df = conn.execute(query_sequence["sales_audit"]).df()

try:

    # This one line runs ALL your business rules at once
    sales_schema.validate(sales_df, lazy=True)
    print("✅ Data Quality Check Passed: Data is safe for AI/Production.")
    
except pa.errors.SchemaErrors as err:

    # 3. GENERATE THE "ERROR REPORT" (This is your value-add)
    print("❌ Data Quality Check Failed!")
    # This returns a dataframe of exactly which rows/cells broke which rules
    report = err.failure_cases 
    report.to_csv("sales_data_health_issues.csv")
    print("Detailed report saved to 'data_health_issues.csv'")

## ====================================================================== ##
## PHASE 2 OF AUDIT: NEGATIVE STOCK & NEVER STOCKED                       ##
## ====================================================================== ##
logs_df = conn.execute(query_sequence["item_audit"]).df()

# Logs Auditing 1: Negative Cumulative Stock
audit_1 = logs_df[logs_df['qty_count'] < 0]\
                    .groupby('pid')\
                    .agg(negative_count=('date', 'count'))\
                    .reset_index()

print(audit_1)

# Logs Auditing 2: Never Stocked Product
audit_2 = logs_df.copy()
audit_2['negatives'] = audit_2['qty_count'].apply(lambda x: 1 if x < 0 else 0)

audit_2 = audit_2.groupby('pid')\
                 .agg(negative_count=('negatives', 'sum'),
                      total_logs=('negatives', 'count'))\
                 .reset_index()
audit_2 = audit_2[audit_2['negative_count'] == audit_2['total_logs']]

print(audit_2)