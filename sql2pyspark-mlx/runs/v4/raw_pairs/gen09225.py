from pyspark.sql import functions as F

result = (
    df_departments
    .filter(~F.col("dept_name").isin(["Engineering", "Legal"]))
    .groupBy(
        F.when(F.col("budget") >= 3090000, "high").when(F.col("budget") >= 1426000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.countDistinct("budget").alias("unique_locations"),
        F.count("budget").alias("budget_count"),
    )
)
