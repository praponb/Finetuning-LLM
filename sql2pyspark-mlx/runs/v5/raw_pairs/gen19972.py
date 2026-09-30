from pyspark.sql import functions as F

order_summary = (
    df_shipments
    .filter(F.col("order_id").isin([6, 31, 32, 39]))
    .groupBy("order_id")
    .agg(
        F.countDistinct("cost").alias("unique_carriers"),
        F.avg("cost").alias("avg_cost"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_summary.alias("t"), F.col("o.id") == F.col("t.order_id"), "inner")
    .select(
        F.col("o.id"),
        F.col("o.customer_id"),
        F.col("o.store_id"),
        F.col("t.unique_carriers"),
        F.col("t.avg_cost"),
    )
    .orderBy(F.col("customer_id").desc())
)
