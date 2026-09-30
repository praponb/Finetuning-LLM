from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 3359000, "high").when(F.col("budget") >= 1266000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg((F.max("budget") - F.min("budget")).alias("budget_range"))
    .filter(F.col("budget_range") < 40)
    .orderBy(F.col("budget_bucket").desc(), F.col("budget_range").desc())
)
