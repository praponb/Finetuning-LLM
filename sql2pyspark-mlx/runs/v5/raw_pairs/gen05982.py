from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(
        F.when(F.col("cost") >= 650, "high").when(F.col("cost") >= 300, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(
        F.count("price").alias("price_count"),
        F.sum(F.when(F.col("category") == "Electronics", F.col("price")).otherwise(0)).alias("electronics_price"),
        F.max("launch_date").alias("last_launch_date"),
    )
)
