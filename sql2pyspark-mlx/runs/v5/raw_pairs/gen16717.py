from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category", F.date_trunc("MONTH", "launch_date").alias("launch_month"))
    .agg(F.round(F.avg("cost"), 2).alias("avg_cost"))
)
