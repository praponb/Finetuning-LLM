from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("price") > 1190)
    .groupBy(
        F.when(F.col("price") >= 910, "high").when(F.col("price") >= 510, "mid").otherwise("low").alias("price_bucket"),
        "category",
    )
    .agg(
        F.sum(F.when(F.col("category") == "Home", F.col("stock_qty")).otherwise(0)).alias("home_stock_qty"),
        F.max("launch_date").alias("latest_launch_date"),
        F.min("launch_date").alias("first_launch_date"),
    )
    .orderBy("price_bucket")
)
