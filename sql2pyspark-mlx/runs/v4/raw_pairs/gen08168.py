from pyspark.sql import functions as F

order_stats = (
    df_shipments
    .groupBy("order_id")
    .agg(
        F.min("shipped_date").alias("first_shipped_date"),
        F.sort_array(F.collect_set("carrier")).alias("carrier_list"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_stats.alias("os"), F.col("o.id") == F.col("os.order_id"), "inner")
    .select(
        F.col("o.id"),
        F.col("o.status"),
        F.col("o.store_id"),
        F.col("os.first_shipped_date"),
        F.col("os.carrier_list"),
    )
    .orderBy(F.col("store_id").desc())
)
