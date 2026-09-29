from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_orders.alias("o"), F.col("o.store_id") == F.col("s.id"), "left")
    .groupBy(F.col("s.id"), F.col("s.store_name"))
    .agg(F.count(F.col("o.id")).alias("order_count"))
    .orderBy(F.col("id").desc(), "store_name")
    .limit(5)
)
