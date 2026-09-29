from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("budget") > 2900000) & (F.col("dept_name").isin(["Engineering", "Sales", "Legal"]) | F.col("location").isin(["Singapore", "London"])))
    .groupBy(
        F.when(F.col("budget") >= 4503000, "high").when(F.col("budget") >= 2030000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        (F.max("budget") - F.min("budget")).alias("budget_range"),
        F.round(F.stddev("budget"), 2).alias("stddev_budget"),
    )
    .orderBy("stddev_budget", "budget_bucket")
    .limit(1)
)
