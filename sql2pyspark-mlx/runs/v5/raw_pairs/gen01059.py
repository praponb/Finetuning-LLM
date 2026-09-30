from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("dept_name")
    .agg(
        F.min("budget").alias("min_budget"),
        F.count(F.when(F.col("location") == "Tokyo", 1)).alias("tokyo_count"),
        F.round(F.stddev("budget"), 2).alias("stddev_budget"),
    )
)
