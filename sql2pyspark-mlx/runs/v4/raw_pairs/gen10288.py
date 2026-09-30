from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("stock_qty") != 332) & F.array_contains("tags", "bestseller") & (F.col("launch_date") >= "2024-02-09"))
    .groupBy(
        F.when(F.col("cost") >= 700, "high").when(F.col("cost") >= 150, "mid").otherwise("low").alias("cost_bucket"),
        "category",
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("category") == "Fashion", 1).otherwise(0))) / F.count("*")), 1).alias("pct_fashion"),
    )
    .orderBy(F.col("pct_fashion").desc())
)
