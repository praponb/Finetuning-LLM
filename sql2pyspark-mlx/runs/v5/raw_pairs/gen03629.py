from pyspark.sql import functions as F

order_summary = (
    df_shipments
    .groupBy("order_id")
    .agg(
        F.max("shipped_date").alias("latest_shipped_date"),
        F.count("cost").alias("cost_count"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_summary.alias("os"), F.col("o.id") == F.col("os.order_id"), "inner")
    .filter(F.col("os.cost_count") >= 5)
    .select(
        F.col("o.id"),
        F.col("o.store_id"),
        F.col("o.amount"),
        F.col("os.latest_shipped_date"),
        F.col("os.cost_count"),
    )
)
