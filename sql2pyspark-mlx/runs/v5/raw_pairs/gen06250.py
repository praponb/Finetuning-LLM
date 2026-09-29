from pyspark.sql import functions as F

order_stats = (
    df_shipments
    .filter(F.col("order_id").isNotNull())
    .groupBy("order_id")
    .agg(
        F.avg("cost").alias("avg_cost"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_stats.alias("os"), F.col("o.id") == F.col("os.order_id"), "left")
    .select(
        F.col("o.id"),
        F.col("o.order_date"),
        F.col("os.avg_cost"),
        F.col("os.status_list"),
    )
    .orderBy(F.col("id").desc())
)
