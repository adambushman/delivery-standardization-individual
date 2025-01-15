# type: ignore
# flake8: noqa
#
#
#
#
#
#
import pandas as pd
import duckdb
#
#
#
#
# Setup DuckDB connection
con = duckdb.connect()

# Reference CSVs
cust_addr_zip_map = 'data/customer_address_and_zip_mapping.csv'
customer_profile = 'data/customer_profile.csv'
transaction_data = 'data/transactional_data.csv'
#
#
#
#
# Load CSVs to persistent tables
con.execute("""
    CREATE TABLE cust_addr_zip_map AS (
        SELECT * FROM read_csv_auto('{cust_addr_zip_map}')
    )
""")
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
duckdb.sql("""
    SELECT FREQUENT_ORDER_TYPE, COUNT(*) AS VOL
    FROM 'data/customer_profile.csv'
    GROUP BY FREQUENT_ORDER_TYPE
    ORDER BY COUNT(*) DESC
""").show()
#
#
#
#
duckdb.sql("""
    SELECT LOCAL_MARKET_PARTNER, COUNT(*) AS VOL
    FROM 'data/customer_profile.csv'
    GROUP BY LOCAL_MARKET_PARTNER
    ORDER BY COUNT(*) DESC
""").show()
#
#
#
#
duckdb.sql("""
    SELECT CO2_CUSTOMER, COUNT(*) AS VOL
    FROM 'data/customer_profile.csv'
    GROUP BY CO2_CUSTOMER
    ORDER BY COUNT(*) DESC
""").show()
#
#
#
