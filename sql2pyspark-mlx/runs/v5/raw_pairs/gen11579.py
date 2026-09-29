from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(F.quarter("launch_date").alias("launch_quarter"), "category")
    .agg(F.round(F.avg("cost"), 2).alias("avg_cost"))
    .orderBy("category", "launch_quarter")
    .limit(5)
)
