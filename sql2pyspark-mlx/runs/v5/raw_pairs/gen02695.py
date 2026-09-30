from pyspark.sql import functions as F

result = (
    df_stores.alias("stores")
    .join(df_orders.alias("orders"), F.col("stores.id") == F.col("orders.store_id"), "left")
    .filter(F.col("stores.city") != "Chiang Mai")
    .groupBy(F.col("stores.id"), F.col("stores.store_name"))
    .agg(F.count(F.col("orders.id")).alias("order_count"))
    .orderBy(F.col("order_count").desc(), "id", "store_name")
    .limit(5)
)
