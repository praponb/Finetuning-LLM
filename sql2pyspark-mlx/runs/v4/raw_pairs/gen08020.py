from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "inner")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .groupBy(F.col("c.is_active"))
    .agg(
        F.count("*").alias("row_count"),
        F.min(F.col("oi.unit_price")).alias("min_unit_price"),
    )
)
