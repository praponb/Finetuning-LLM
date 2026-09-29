from pyspark.sql import functions as F

result = (
    df_employees.alias("employees")
    .join(df_departments.alias("departments"), F.col("employees.dept_id") == F.col("departments.id"), "left")
    .filter(F.col("departments.budget") >= 1088000)
    .select(
        F.col("employees.id"),
        F.col("employees.is_remote"),
        F.col("employees.full_name"),
        F.col("departments.budget"),
    )
    .orderBy("id")
    .limit(20)
)
