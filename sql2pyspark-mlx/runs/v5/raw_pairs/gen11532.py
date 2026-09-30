from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location") == "Berlin")
    .groupBy(
        F.when(F.col("budget") >= 3520000, "high").when(F.col("budget") >= 847000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.min("budget").alias("min_budget"))
    .orderBy(F.col("budget_bucket").desc())
)
