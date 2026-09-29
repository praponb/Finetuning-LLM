from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isin(["Berlin", "London"]))
    .groupBy("dept_name")
    .agg(F.count(F.when(F.col("dept_name") == "HR", 1)).alias("hr_count"))
    .filter(F.col("hr_count") < 8)
    .select("dept_name", "hr_count")
)
