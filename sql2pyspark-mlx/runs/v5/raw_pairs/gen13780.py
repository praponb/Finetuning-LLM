from pyspark.sql import functions as F

result = (
    df_departments
    .filter(~F.col("location").isin(["London", "Berlin"]))
    .groupBy("location")
    .agg(
        F.sum("budget").alias("total_budget"),
        F.count("budget").alias("budget_count"),
    )
)
