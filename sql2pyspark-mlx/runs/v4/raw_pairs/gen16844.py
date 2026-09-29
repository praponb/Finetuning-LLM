from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isin(["Marketing", "Finance"]))
    .groupBy("dept_name")
    .agg(
        F.sum(F.when(F.col("dept_name") == "HR", F.col("budget")).otherwise(0)).alias("hr_budget"),
        F.count("*").alias("department_count"),
        F.sum(F.when(F.col("dept_name") == "Finance", 1).otherwise(0)).alias("finance_count"),
    )
    .filter(F.col("finance_count") >= 40)
    .drop("finance_count")
)
