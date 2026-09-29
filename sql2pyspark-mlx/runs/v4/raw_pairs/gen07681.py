from pyspark.sql import functions as F

result = (
    df_departments.alias("departments")
    .join(df_employees.alias("employees"), F.col("employees.dept_id") == F.col("departments.id"), "left")
    .filter(F.col("employees.full_name").like("FR%"))
    .groupBy(F.col("departments.dept_name"))
    .agg(F.count(F.col("employees.id")).alias("employee_count"))
    .orderBy("dept_name")
)
