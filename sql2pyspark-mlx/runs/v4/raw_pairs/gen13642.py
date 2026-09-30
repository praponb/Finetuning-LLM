from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .groupBy(
        F.when(F.col("p.stock_qty") >= 398, "high").when(F.col("p.stock_qty") >= 87, "mid").otherwise("low").alias("stock_qty_bucket"),
    )
    .agg(
        F.sum(F.col("oi.unit_price")).alias("total_unit_price"),
        F.count("*").alias("total_count"),
    )
)
