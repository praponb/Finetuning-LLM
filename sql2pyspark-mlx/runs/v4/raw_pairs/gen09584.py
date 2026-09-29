from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.array_contains("tags", "bestseller"))
    .rollup("category", F.date_format("launch_date", "yyyy-MM").alias("launch_month"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("brand") == "Umbrella", 1).otherwise(0))) / F.count("*")), 1).alias("pct_umbrella"),
    )
    .orderBy(F.col("launch_month").desc(), F.col("pct_umbrella").desc())
)
