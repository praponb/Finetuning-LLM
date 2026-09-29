from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .groupBy(F.col("o.status"), F.col("o.channel"))
    .agg(F.count("*").alias("total_count"))
    .orderBy("channel", "status")
)
