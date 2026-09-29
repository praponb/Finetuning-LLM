from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isin(["Chiang Mai", "Berlin"]) & F.col("dept_name").isNull())
    .groupBy(
        F.when(F.col("budget") >= 3564000, "high").when(F.col("budget") >= 1312000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("dept_name") == "HR", 1).otherwise(0))) / F.count("*")), 1).alias("pct_hr"),
        F.count("*").alias("department_count"),
    )
    .filter(F.col("pct_hr") < 38)
    .orderBy("budget_bucket")
    .limit(5)
)
