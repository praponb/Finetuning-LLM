from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("dept_name")
    .agg(
        F.percentile_approx("budget", [0.25, 0.5, 0.75]).alias("budget_quartiles"),
        F.sum(F.when(F.col("location") == "London", F.col("budget")).otherwise(0)).alias("london_budget"),
        F.sum(F.when(F.col("location") == "Berlin", F.col("budget")).otherwise(0)).alias("berlin_budget"),
        F.round(F.avg("budget"), 2).alias("avg_budget"),
        F.count_if(F.col("location") == "Berlin").alias("berlin_events"),
    )
    .orderBy(F.col("london_budget").desc_nulls_first())
)
