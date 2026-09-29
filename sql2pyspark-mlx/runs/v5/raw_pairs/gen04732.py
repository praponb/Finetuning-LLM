from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id") == 37)
    .groupBy(
        F.when(F.col("brand").isin(["Stark", "Initech"]), "tier_1").otherwise("other").alias("brand_group"),
    )
    .agg(
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
        F.min("stock_qty").alias("min_stock_qty"),
    )
    .orderBy(F.col("min_stock_qty").desc(), "brand_group")
    .limit(10)
)
