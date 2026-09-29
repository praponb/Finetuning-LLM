from pyspark.sql import functions as F

result = (
    df_departments.alias("departments")
    .join(df_employees.alias("employees"), F.col("employees.dept_id") == F.col("departments.id"), "left")
    .filter(F.col("employees.manager_id") == 26)
    .select(
        F.col("departments.budget"),
        F.col("departments.dept_name"),
        F.col("departments.location"),
        F.col("employees.job_title"),
    )
    .orderBy(F.col("budget").desc())
)
