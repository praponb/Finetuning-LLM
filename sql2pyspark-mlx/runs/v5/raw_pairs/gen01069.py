from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isin(["Marketing", "Finance", "Engineering"]))
    .groupBy(
        F.when(F.col("budget") >= 3973000, "high").when(F.col("budget") >= 1158000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("location") == "New York", 1).otherwise(0)).alias("new_york_count"),
    )
    .filter(F.col("new_york_count") < 24)
    .select("budget_bucket", "new_york_count")
    .orderBy(F.col("new_york_count").desc(), "budget_bucket")
    .limit(3)
)
