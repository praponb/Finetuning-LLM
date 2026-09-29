from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isin(["HR", "Marketing"]) | F.col("dept_name").isin(["Sales", "Finance", "Legal"]))
    .groupBy(
        F.when(F.col("budget") >= 4288000, "high").when(F.col("budget") >= 1621000, "mid").otherwise("low").alias("budget_bucket"),
        "location",
    )
    .agg(
        F.max("budget").alias("max_budget"),
        F.avg("budget").alias("avg_budget"),
    )
    .filter(F.col("avg_budget") >= 2808000)
    .drop("avg_budget")
    .orderBy(F.col("max_budget").desc())
)
