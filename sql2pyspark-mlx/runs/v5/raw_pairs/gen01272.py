from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isin(["London", "Singapore", "Chiang Mai"]))
    .groupBy("location")
    .agg(
        F.sum(F.when(F.col("location") == "New York", F.col("budget")).otherwise(0)).alias("new_york_budget"),
        F.round(F.avg("budget"), 2).alias("avg_budget"),
    )
    .orderBy("avg_budget", "location")
    .limit(1)
)
