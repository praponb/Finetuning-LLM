from pyspark.sql import functions as F

recent_products = (
    df_products
    .filter(F.array_contains("tags", "bestseller"))
    .withColumn("product_name_normalized", F.upper(F.trim("product_name")))
)

category_summary = (
    recent_products
    .groupBy(
        "category",
        F.when(F.col("stock_qty") >= 300, "high").when(F.col("stock_qty") >= 51, "mid").otherwise("low").alias("stock_qty_bucket"),
    )
    .agg(
        F.countDistinct("stock_qty").alias("unique_categorys"),
        F.min("cost").alias("min_cost"),
    )
)

result = category_summary
