from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "left")
    .groupBy(F.col("o.customer_id"), F.col("o.channel"))
    .agg(
        (F.max(F.col("oi.unit_price")) - F.min(F.col("oi.unit_price"))).alias("unit_price_range"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("unit_price_range") > 37)
)
