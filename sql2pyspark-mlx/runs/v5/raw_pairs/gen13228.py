from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .groupBy(F.date_trunc("MONTH", F.col("o.order_date")).alias("order_month"))
    .agg(
        F.count("*").alias("total_count"),
        F.max(F.col("s.delivered_date")).alias("last_delivered_date"),
    )
    .orderBy("order_month")
)
