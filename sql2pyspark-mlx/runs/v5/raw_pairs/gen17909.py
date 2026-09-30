from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isin(["London", "Bangkok"]))
    .groupBy(
        F.when(F.col("budget") >= 4051000, "high").when(F.col("budget") >= 1798000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("location") == "Singapore", 1).otherwise(0))) / F.count("*")), 1).alias("pct_singapore"),
    )
    .orderBy(F.col("pct_singapore").desc())
)
