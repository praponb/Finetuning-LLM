from pyspark.sql import functions as F

result = (
    df_departments.alias("departments")
    .join(df_employees.alias("employees"), F.col("departments.id") == F.col("employees.dept_id"), "left")
    .filter(F.col("employees.manager_id") == 6)
    .select(F.col("departments.budget"), F.col("employees.dept_id"))
)
