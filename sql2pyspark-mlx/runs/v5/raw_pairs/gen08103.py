from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("location", "dept_name")
    .agg(
        F.count("budget").alias("budget_count"),
        F.sum("budget").alias("total_budget"),
        F.sum(F.when(F.col("dept_name") == "Marketing", 1).otherwise(0)).alias("marketing_count"),
    )
    .filter(F.col("marketing_count") >= 39)
    .drop("marketing_count")
    .orderBy("budget_count", "location", "dept_name")
    .limit(1)
)
