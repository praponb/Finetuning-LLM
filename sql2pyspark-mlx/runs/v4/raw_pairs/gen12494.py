from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget").isNotNull() & (F.col("dept_name") == "Marketing") & (F.col("location") == "Chiang Mai"))
    .groupBy(
        F.when(F.col("budget") >= 4280000, "high").when(F.col("budget") >= 1862000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.round(F.stddev("budget"), 2).alias("stddev_budget"))
)
