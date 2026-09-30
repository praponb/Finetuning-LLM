from pyspark.sql import functions as F

valid_departments = df_departments.filter(F.col("dept_name").isin(["Marketing", "Finance"]))

dept_name_summary = (
    valid_departments
    .groupBy("dept_name")
    .agg(F.sum(F.when(F.col("dept_name") == "HR", 1).otherwise(0)).alias("hr_count"))
)

result = dept_name_summary.orderBy(F.col("hr_count").desc())
