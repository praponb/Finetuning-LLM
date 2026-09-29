from pyspark.sql import functions as F

result = (
    df_employees.alias("employees")
    .join(df_departments.alias("departments"), F.col("employees.dept_id") == F.col("departments.id"), "inner")
    .select(
        F.col("employees.id"),
        F.col("employees.hire_date"),
        F.col("employees.email"),
        F.col("employees.full_name"),
        F.col("departments.budget"),
    )
    .orderBy(F.col("id").desc())
    .limit(50)
)
