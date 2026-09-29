from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id").isin([23, 26, 31]))
    .groupBy(
        F.when(F.col("price") >= 1180, "high").when(F.col("price") >= 490, "mid").otherwise("low").alias("price_bucket"),
        "brand",
    )
    .agg(
        F.sum("stock_qty").alias("total_stock_qty"),
        F.avg("stock_qty").alias("avg_stock_qty"),
    )
    .filter(F.col("total_stock_qty") >= 1400)
    .select("price_bucket", "brand", "total_stock_qty", "avg_stock_qty")
)
