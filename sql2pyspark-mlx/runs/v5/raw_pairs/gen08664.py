from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category", F.quarter("launch_date").alias("launch_quarter"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("brand") == "Umbrella", 1).otherwise(0))) / F.count("*")), 1).alias("pct_umbrella"),
    )
    .filter(F.col("pct_umbrella") >= 38)
    .select("category", "launch_quarter", "pct_umbrella")
    .orderBy(F.col("category").desc())
)
