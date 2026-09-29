from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(F.month("launch_date").alias("launch_month"))
    .agg(
        F.sum(F.when(F.col("brand") == "Umbrella", 1).otherwise(0)).alias("umbrella_count"),
        F.round(((100 * F.sum(F.when(F.col("category") == "Home", 1).otherwise(0))) / F.count("*")), 1).alias("pct_home"),
    )
)
