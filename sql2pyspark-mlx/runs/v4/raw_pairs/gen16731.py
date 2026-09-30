from pyspark.sql import functions as F

order_stats = (
    df_order_items
    .filter(F.col("product_id") == 1)
    .groupBy("order_id")
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.count("*").alias("row_count"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_stats.alias("os"), F.col("o.id") == F.col("os.order_id"), "left")
    .select(
        F.col("o.customer_id"),
        F.col("o.amount"),
        F.col("os.total_quantity"),
        F.col("os.row_count"),
    )
)
