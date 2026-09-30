from pyspark.sql import functions as F

result = (
    df_departments.alias("departments")
    .join(df_employees.alias("employees"), F.col("departments.id") == F.col("employees.dept_id"), "left")
    .filter(F.col("employees.salary") > 76000)
    .select(
        F.col("departments.budget"),
        F.col("employees.full_name"),
        F.col("employees.hire_date"),
    )
    .orderBy("full_name")
)
