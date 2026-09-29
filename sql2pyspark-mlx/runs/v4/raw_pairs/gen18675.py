from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(F.quarter("launch_date").alias("launch_quarter"))
    .agg(
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.round(((100 * F.sum(F.when(F.col("category") == "Home", 1).otherwise(0))) / F.count("*")), 1).alias("pct_home"),
    )
)
