from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(
        F.when(F.col("price") >= 1280, "high").when(F.col("price") >= 310, "mid").otherwise("low").alias("price_bucket"),
        "category",
    )
    .agg(
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.count(F.when(F.col("category") == "Home", 1)).alias("home_count"),
    )
)
