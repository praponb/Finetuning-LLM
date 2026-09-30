from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 4020000, "high").when(F.col("budget") >= 1003000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg((F.max("budget") - F.min("budget")).alias("budget_range"))
    .filter(F.col("budget_range") > 55)
    .orderBy(F.col("budget_bucket").desc())
)
