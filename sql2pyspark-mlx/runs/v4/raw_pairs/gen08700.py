from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("budget") >= 2193000) & ~F.col("location").isin(["Chiang Mai", "Bangkok", "New York"]))
    .groupBy(
        F.when(F.col("budget") >= 3847000, "high").when(F.col("budget") >= 1150000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.round(F.stddev("budget"), 2).alias("stddev_budget"),
        F.countDistinct("budget").alias("unique_locations"),
        F.sum(F.when(F.col("dept_name") == "Engineering", F.col("budget")).otherwise(0)).alias("engineering_budget"),
    )
    .filter(F.col("stddev_budget") >= 41)
)
