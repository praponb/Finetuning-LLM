from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 3938000, "high").when(F.col("budget") >= 1005000, "mid").otherwise("low").alias("budget_bucket"),
        "location",
    )
    .agg(F.max("budget").alias("max_budget"))
    .filter(F.col("max_budget") < 1571000)
    .orderBy("budget_bucket", F.col("location").desc())
)
