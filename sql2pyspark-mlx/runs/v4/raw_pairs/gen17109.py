from pyspark.sql import functions as F

result = (
    df_employees.alias("employees")
    .join(df_departments.filter(F.col("location") == "Bangkok").alias("departments"), F.col("employees.dept_id") == F.col("departments.id"), "left_anti")
    .select(
        F.col("employees.id"),
        F.col("employees.job_title"),
        F.col("employees.hire_date"),
        F.col("employees.is_remote"),
    )
    .orderBy(F.col("id").desc())
)
