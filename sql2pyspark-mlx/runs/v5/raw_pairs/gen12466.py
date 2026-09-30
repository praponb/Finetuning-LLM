from pyspark.sql import functions as F

orders_by_store = (
    df_orders
    .filter(F.col("store_id").isNotNull())
    .groupBy("store_id")
    .agg(
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
        F.count("*").alias("num_orders"),
    )
)

result = (
    df_stores.alias("s")
    .join(orders_by_store.alias("O"), F.col("s.id") == F.col("O.store_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.store_name"),
        F.col("O.channel_list"),
        F.col("O.num_orders"),
    )
    .orderBy("store_name", "id")
)
