from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.filter(F.col("location").isin(["Berlin", "Singapore", "New York"])).alias("d"), F.col("e.dept_id") == F.col("d.id"), "left_anti")
    .filter(F.col("e.job_title").isin(["Engineer", "Director", "Designer"]))
    .select(F.col("e.id"), F.col("e.level"))
)
