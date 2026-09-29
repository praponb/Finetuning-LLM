from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_orders.alias("o"), F.col("o.store_id") == F.col("s.id"), "left")
    .filter((F.col("o.order_date") < "2024-07-01") | (F.col("s.city") == "Tokyo"))
    .select(F.col("s.store_name"), F.col("o.channel"))
    .orderBy("store_name")
)
