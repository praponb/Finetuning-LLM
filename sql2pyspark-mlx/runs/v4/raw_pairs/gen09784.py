from pyspark.sql import functions as F

product_stats = (
    df_order_items
    .groupBy("product_id")
    .agg(
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
        F.sum("unit_price").alias("total_unit_price"),
    )
)

result = (
    df_products.alias("p")
    .join(product_stats.alias("P"), F.col("p.id") == F.col("P.product_id"), "inner")
    .select(
        F.col("p.stock_qty"),
        F.col("P.avg_unit_price"),
        F.col("P.total_unit_price"),
    )
    .orderBy(F.col("avg_unit_price").desc())
)
