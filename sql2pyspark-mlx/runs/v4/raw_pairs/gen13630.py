from pyspark.sql import functions as F

result = (
    df_departments.alias("departments")
    .join(df_employees.alias("employees"), F.col("departments.id") == F.col("employees.dept_id"), "inner")
    .filter(F.col("employees.manager_id").isin([4, 17, 32, 33]) & (F.col("departments.budget") > 1832000))
    .select(
        F.col("departments.budget"),
        F.col("employees.level"),
        F.col("employees.hire_date"),
    )
)
