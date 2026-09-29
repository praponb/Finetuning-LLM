from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        "location",
        F.when(F.col("budget") >= 3267000, "high").when(F.col("budget") >= 606000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.min("budget").alias("min_budget"))
    .orderBy("location")
)
