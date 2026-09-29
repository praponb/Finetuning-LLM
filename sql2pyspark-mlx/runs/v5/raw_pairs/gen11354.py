from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "left")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "right")
    .groupBy(
        F.when(F.col("o.amount") >= 1600, "high").when(F.col("o.amount") >= 680, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(F.count(F.col("oi.quantity")).alias("quantity_count"))
    .filter(F.col("quantity_count") >= 5)
)
