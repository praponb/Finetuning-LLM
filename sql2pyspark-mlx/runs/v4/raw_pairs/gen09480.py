from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .filter(F.col("oi.order_id").isin([14, 15, 17, 39]))
    .groupBy(F.date_trunc("MONTH", F.col("o.order_date")).alias("order_month"))
    .agg(
        F.avg(F.col("oi.quantity")).alias("avg_quantity"),
        F.count("*").alias("cnt"),
    )
    .orderBy("order_month")
)
