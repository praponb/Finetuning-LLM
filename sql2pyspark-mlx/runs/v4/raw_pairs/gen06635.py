from pyspark.sql import functions as F

order_summary = (
    df_shipments
    .groupBy("order_id")
    .agg(
        F.countDistinct("cost").alias("unique_statuss"),
        F.round(((100 * F.sum(F.when(F.col("status") == "LOST", 1).otherwise(0))) / F.count("*")), 1).alias("pct_lost"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_summary.alias("os"), F.col("o.id") == F.col("os.order_id"), "left")
    .filter(F.col("os.unique_statuss") >= 9)
    .select(
        F.col("o.id"),
        F.col("o.discount"),
        F.col("o.shipping_fee"),
        F.col("os.unique_statuss"),
        F.col("os.pct_lost"),
    )
    .orderBy("pct_lost")
)
