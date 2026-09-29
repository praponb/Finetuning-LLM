from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("dept_name", "location")
    .agg(
        F.count("*").alias("department_count"),
        F.sum(F.when(F.col("dept_name") == "HR", F.col("budget")).otherwise(0)).alias("hr_budget"),
    )
    .filter(F.col("hr_budget") < 67)
)
