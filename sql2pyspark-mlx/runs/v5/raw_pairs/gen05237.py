from pyspark.sql import functions as F

orders_by_store = (
    df_orders
    .groupBy("store_id")
    .agg(F.sort_array(F.collect_set("channel")).alias("channel_list"))
)

result = (
    df_stores.alias("s")
    .join(orders_by_store.alias("t"), F.col("s.id") == F.col("t.store_id"), "inner")
    .select(F.col("s.id"), F.col("s.sqft"), F.col("t.channel_list"))
    .orderBy(F.col("sqft").desc())
)
