from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        "location",
        F.when(F.col("budget") >= 3624000, "high").when(F.col("budget") >= 1251000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.round(F.stddev("budget"), 2).alias("stddev_budget"))
    .filter(F.col("stddev_budget") > 32)
    .select("location", "budget_bucket", "stddev_budget")
    .orderBy("budget_bucket", "location")
)
