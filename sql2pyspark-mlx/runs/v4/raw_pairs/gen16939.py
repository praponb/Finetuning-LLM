from pyspark.sql import functions as F

result = (
    df_departments.alias("departments")
    .join(df_employees.alias("employees"), F.col("employees.dept_id") == F.col("departments.id"), "left")
    .filter((F.length(F.col("employees.full_name")) > 7) & ~F.col("employees.is_remote"))
    .select(F.col("departments.dept_name"), F.col("employees.full_name"))
)
