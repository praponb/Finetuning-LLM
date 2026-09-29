from pyspark.sql import functions as F

order_summary = (
    df_shipments
    .groupBy("order_id")
    .agg(
        F.countDistinct("cost").alias("unique_statuss"),
        F.round(F.avg("cost"), 2).alias("avg_cost"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_summary.alias("os"), F.col("o.id") == F.col("os.order_id"), "inner")
    .select(
        F.col("o.id"),
        F.col("o.shipping_fee"),
        F.col("os.unique_statuss"),
        F.col("os.avg_cost"),
    )
)
