from pyspark.sql import functions as F

valid_departments = df_departments.filter(F.col("dept_name") == "Finance")

departments_stats = (
    valid_departments
    .groupBy(
        "location",
        F.when(F.col("budget") >= 3222000, "high").when(F.col("budget") >= 778000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.round(F.stddev("budget"), 2).alias("stddev_budget"),
        F.sum(F.when(F.col("location") == "London", 1).otherwise(0)).alias("london_count"),
    )
)

result = (
    departments_stats
    .filter(F.col("stddev_budget") < 80)
    .orderBy("location")
)
