from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("location")
    .agg(
        F.avg("budget").alias("avg_budget"),
        F.max("budget").alias("max_budget"),
        F.count("*").alias("num_departments"),
    )
)
