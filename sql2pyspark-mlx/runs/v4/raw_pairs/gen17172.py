from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .groupBy(
        F.when(F.col("o.amount") >= 1320, "high").when(F.col("o.amount") >= 610, "mid").otherwise("low").alias("amount_bucket"),
        F.col("o.channel"),
    )
    .agg(F.max(F.col("oi.quantity")).alias("max_quantity"))
    .orderBy(F.col("amount_bucket").desc(), F.col("max_quantity").desc())
)
