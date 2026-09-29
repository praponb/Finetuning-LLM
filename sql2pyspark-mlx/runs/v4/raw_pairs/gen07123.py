from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(F.month("launch_date").alias("launch_month"))
    .agg(
        F.min("launch_date").alias("min_launch_date"),
        F.count("cost").alias("cost_count"),
    )
    .filter(F.col("cost_count") < 6)
    .select("launch_month", "min_launch_date", "cost_count")
    .orderBy("cost_count")
)
