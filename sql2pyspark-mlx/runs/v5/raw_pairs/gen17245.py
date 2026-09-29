from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .filter(F.col("o.customer_id").isin([6, 34]))
    .groupBy(
        F.col("o.status"),
        F.date_format(F.col("o.order_date"), "yyyy-MM").alias("order_month"),
    )
    .agg(
        (F.max(F.col("oi.quantity")) - F.min(F.col("oi.quantity"))).alias("quantity_range"),
        F.count("*").alias("total_count"),
    )
)
