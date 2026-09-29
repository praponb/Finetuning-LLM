from pyspark.sql import functions as F

valid_departments = (
    df_departments
    .filter(~((F.col("dept_name") == "HR") | F.col("dept_name").isin(["Sales", "Engineering", "HR"])))
)

summary = (
    valid_departments
    .groupBy("location")
    .agg(
        F.count(F.when(F.col("location") == "Singapore", 1)).alias("singapore_count"),
        F.sum("budget").alias("total_budget"),
    )
)

result = summary.filter(F.col("singapore_count") >= 5)
