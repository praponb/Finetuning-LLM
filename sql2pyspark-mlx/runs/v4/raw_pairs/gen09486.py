from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget").between(1563000, 3826000))
    .groupBy(
        F.when(F.col("budget") >= 3756000, "high").when(F.col("budget") >= 1409000, "mid").otherwise("low").alias("budget_bucket"),
        "location",
    )
    .agg(F.round(F.avg("budget"), 2).alias("avg_budget"))
    .orderBy("location")
)
