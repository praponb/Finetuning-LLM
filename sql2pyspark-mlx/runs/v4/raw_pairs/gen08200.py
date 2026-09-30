from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("stock_qty") != 253) & F.month("launch_date").isin([7, 9, 10]))
    .groupBy(
        "supplier_id",
        "brand",
        F.when(F.col("price") >= 1180, "high").when(F.col("price") >= 540, "mid").otherwise("low").alias("price_bucket"),
    )
    .agg(
        F.min("price").alias("min_price"),
        F.sum(F.when(F.col("category") == "Home", 1).otherwise(0)).alias("home_count"),
    )
    .orderBy(F.col("price_bucket").desc())
)
