from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        "dept_name",
        "location",
        F.when(F.col("budget") >= 3672000, "high").when(F.col("budget") >= 1680000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.max("budget").alias("max_budget"),
        F.min("budget").alias("min_budget"),
        F.countDistinct("budget").alias("unique_dept_names"),
    )
    .orderBy(F.col("budget_bucket").desc(), "dept_name", "location")
    .limit(3)
)
