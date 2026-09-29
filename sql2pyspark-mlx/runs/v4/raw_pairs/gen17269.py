from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_orders.filter(F.col("discount").between(0.11, 0.19)).alias("o"), F.col("s.id") == F.col("o.store_id"), "left_anti")
    .filter(F.upper(F.trim(F.col("s.store_name"))) == "AIRPORT KIOSK")
    .select(F.col("s.id"), F.col("s.store_name"), F.col("s.region"))
)
