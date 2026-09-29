from pyspark.sql import functions as F

result = (
    df_departments
    .filter(~F.col("location").isin(["London", "Chiang Mai", "Singapore"]))
    .groupBy(
        F.when(F.col("budget") >= 4056000, "high").when(F.col("budget") >= 1036000, "mid").otherwise("low").alias("budget_bucket"),
        "location",
    )
    .agg(
        F.sum(F.when(F.col("location") == "Bangkok", F.col("budget")).otherwise(0)).alias("bangkok_budget"),
    )
    .orderBy("location")
)
