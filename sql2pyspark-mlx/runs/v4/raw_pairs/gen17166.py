from pyspark.sql import functions as F

recent_products = (
    df_products
    .filter((F.col("price") > 590) & (F.col("launch_date") < "2024-08-01") & (F.col("category") == "Books"))
)

summary = (
    recent_products
    .groupBy(
        F.when(F.col("cost") >= 690, "high").when(F.col("cost") >= 310, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(F.count("*").alias("num_products"))
)

result = (
    summary
    .select("cost_bucket", "num_products")
    .orderBy(F.col("cost_bucket").desc())
)
