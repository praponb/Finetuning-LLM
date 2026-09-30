from pyspark.sql import functions as F

result = (
    df_departments.alias("departments")
    .join(df_employees.alias("employees"), F.col("departments.id") == F.col("employees.dept_id"), "left_semi")
    .filter(F.col("departments.budget") > 3982000)
    .select(F.col("departments.id"), F.col("departments.budget"))
    .orderBy(F.col("budget").desc())
)
