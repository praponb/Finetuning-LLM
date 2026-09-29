from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isin(["London", "Bangkok", "Singapore"]) & (F.col("location") == "Chiang Mai"))
    .agg(
        F.max("budget").alias("max_budget"),
        F.count("budget").alias("budget_count"),
    )
    .orderBy("budget_count", "max_budget")
)
