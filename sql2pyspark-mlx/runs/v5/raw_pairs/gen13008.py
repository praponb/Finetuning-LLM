from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("cost").isNotNull() & (F.col("price") >= 460)) | (F.col("stock_qty") == 350))
    .groupBy(
        "category",
        F.when(F.col("stock_qty") >= 346, "high").when(F.col("stock_qty") >= 191, "mid").otherwise("low").alias("stock_qty_bucket"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("category") == "Home", 1).otherwise(0))) / F.count("*")), 1).alias("pct_home"),
        F.countDistinct("price").alias("unique_brands"),
        F.min("price").alias("min_price"),
    )
    .filter(F.col("min_price") > 880)
    .drop("min_price")
    .orderBy("category")
)
