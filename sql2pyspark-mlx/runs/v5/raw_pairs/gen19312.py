from pyspark.sql import functions as F

result = (
    df_employees.alias("employees")
    .join(df_departments.alias("departments"), F.col("employees.dept_id") == F.col("departments.id"), "left")
    .filter(F.col("employees.is_remote"))
    .groupBy(F.col("departments.dept_name"))
    .agg(F.avg(F.col("employees.salary")).alias("avg_salary"))
)
