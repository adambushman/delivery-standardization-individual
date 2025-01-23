# type: ignore
# flake8: noqa
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
#
import polars as pl
import duckdb
import skimpy
import folium

from plotnine import ggplot, aes
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
con.execute(f"""
    CREATE TABLE cust_addr AS (
        SELECT * FROM read_csv_auto('{cust_addr_zip_map}')
    );

    CREATE TABLE cust_profile AS (
        SELECT * FROM read_csv_auto('{customer_profile}')
    );

    CREATE TABLE transactions AS (
        SELECT * FROM read_csv_auto('{transaction_data}')
    );
""")
#
#
#
#
con.sql("SHOW TABLES")
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
cust_profile_df = con.sql("FROM cust_profile").pl()
#
#
#
#
#
con.sql("DESCRIBE cust_profile")
#
#
#
#
#
cust_profile_df.null_count()
#
#
#
#
#
#
#
con.sql("""
    SELECT
    COUNT(DISTINCT COALESCE(PRIMARY_GROUP_NUMBER, CUSTOMER_NUMBER)) AS TOT_CUST
    FROM cust_profile
""")
#
#
#
#
#
con.sql("""
    SELECT
    COALESCE(PRIMARY_GROUP_NUMBER, CUSTOMER_NUMBER) AS CUST_ID
    ,COUNT(*)
    FROM cust_profile
    GROUP BY ALL
    ORDER BY COUNT(*) DESC
""")
#
#
#
#
#
con.sql("""
    SELECT FREQUENT_ORDER_TYPE, COUNT(*) AS VOL
    FROM cust_profile
    GROUP BY FREQUENT_ORDER_TYPE
    ORDER BY COUNT(*) DESC
""")
#
#
#
con.sql("""
    SELECT COLD_DRINK_CHANNEL, COUNT(*) AS VOL
    FROM cust_profile
    GROUP BY COLD_DRINK_CHANNEL
    ORDER BY COUNT(*) DESC
""")
#
#
#
#
#
con.sql("""
    SELECT LOCAL_MARKET_PARTNER, COUNT(*) AS VOL
    FROM cust_profile
    GROUP BY LOCAL_MARKET_PARTNER
    ORDER BY COUNT(*) DESC
""")
#
#
#
#
#
con.sql("""
    SELECT
    LOCAL_MARKET_PARTNER,
    SUM(CASE WHEN PRIMARY_GROUP_NUMBER IS NULL THEN 1 ELSE 0 END) AS CUST,
    SUM(CASE WHEN PRIMARY_GROUP_NUMBER IS NULL THEN 0 ELSE 1 END) AS GROUP
    FROM cust_profile
    GROUP BY LOCAL_MARKET_PARTNER
""")
#
#
#
#
#
#
#
con.sql("""
    SELECT CO2_CUSTOMER, COUNT(*) AS VOL
    FROM cust_profile
    GROUP BY CO2_CUSTOMER
    ORDER BY COUNT(*) DESC
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
cust_addr_df = con.sql("FROM cust_addr").pl()
#
#
#
#
#
con.sql("DESCRIBE cust_addr")
#
#
#
#
#
con.execute("""
    CREATE TABLE cust_addr_detail AS (
        SELECT
        zip
        ,LIST_ELEMENT(STRING_SPLIT("full address", ','), 2) AS city
        ,LIST_ELEMENT(STRING_SPLIT("full address", ','), 3) AS state
        ,LIST_ELEMENT(STRING_SPLIT("full address", ','), 4) AS state_abbr
        ,LIST_ELEMENT(STRING_SPLIT("full address", ','), 5) AS county
        ,CAST(LIST_ELEMENT(STRING_SPLIT("full address", ','), 7) AS DOUBLE) AS lat
        ,CAST(LIST_ELEMENT(STRING_SPLIT("full address", ','), 8) AS DOUBLE) AS lon
        FROM cust_addr
    )
""")
#
#
#
con.sql("DESCRIBE cust_addr_detail")
#
#
#
#
#
con.sql("""
    SELECT 
    cad.state
    ,COUNT(*)
    FROM cust_profile cp
    INNER JOIN cust_addr_detail cad ON cad.zip = cp.ZIP_CODE
    GROUP BY cad.state
""")
#
#
#
#
#
cust_addr = con.sql("SELECT * FROM cust_addr_detail").pl()
#
#
#
swire_map = folium.Map(
    location = [
        cust_addr['lat'].mean(),
        cust_addr['lon'].mean()
    ],
    zoom_start = 2.75, 
    control_scale = True
)
#
#
#
for row in cust_addr.iter_rows():
    folium.Marker(
        location = [row[5], row[6]], 
        icon = folium.Icon(color = "red")
    ).add_to(swire_map)
#
#
#
swire_map
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
cust_addr_df = con.sql("FROM transactions").pl()
#
#
#
#
#
con.sql("DESCRIBE transactions")
#
#
#
#
#
