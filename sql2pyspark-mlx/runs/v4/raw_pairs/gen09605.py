from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "inner")
    .groupBy(
        F.year(F.col("p.launch_date")).alias("launch_year"),
        F.when(F.col("p.price") >= 1050, "high").when(F.col("p.price") >= 480, "mid").otherwise("low").alias("price_bucket"),
    )
    .agg(F.min(F.col("oi.quantity")).alias("min_quantity"))
    .filter(F.col("min_quantity") >= 6.8)
    .orderBy(F.col("launch_year").desc())
)
