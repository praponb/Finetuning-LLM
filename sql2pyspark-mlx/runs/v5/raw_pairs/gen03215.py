from pyspark.sql import functions as F

result = (
    df_employees.alias("employees")
    .join(df_departments.alias("departments"), F.col("employees.dept_id") == F.col("departments.id"), "inner")
    .filter(F.col("departments.location") == "Bangkok")
    .select(F.col("employees.hire_date"), F.col("departments.dept_name"))
)
