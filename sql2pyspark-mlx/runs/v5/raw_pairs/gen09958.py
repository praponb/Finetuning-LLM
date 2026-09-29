from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 4085000, "high").when(F.col("budget") >= 1012000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        (F.max("budget") - F.min("budget")).alias("budget_range"),
        F.round(((100 * F.sum(F.when(F.col("dept_name") == "Engineering", 1).otherwise(0))) / F.count("*")), 1).alias("pct_engineering"),
    )
    .filter(F.col("pct_engineering") >= 40)
    .select("budget_bucket", "budget_range", "pct_engineering")
    .orderBy("pct_engineering")
)
