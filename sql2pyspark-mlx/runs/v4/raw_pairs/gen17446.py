from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 3913000, "high").when(F.col("budget") >= 1471000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg((F.max("budget") - F.min("budget")).alias("budget_range"))
    .filter(F.col("budget_range") > 50)
    .orderBy(F.col("budget_range").desc(), "budget_bucket")
    .limit(5)
)
