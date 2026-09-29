from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "inner")
    .filter(F.col("oi.unit_price") > 170)
    .groupBy(F.col("o.status"), F.col("o.customer_id"))
    .agg(
        F.count("*").alias("num_order_items"),
        F.min(F.col("oi.quantity")).alias("min_quantity"),
    )
    .orderBy(F.col("num_order_items").desc())
)
