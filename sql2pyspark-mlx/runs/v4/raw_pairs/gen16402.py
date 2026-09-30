from pyspark.sql import functions as F

result = (
    df_departments.alias("departments")
    .join(df_employees.filter(F.col("full_name").like("%Carol%")).alias("employees"), F.col("departments.id") == F.col("employees.dept_id"), "left_semi")
    .select(F.col("departments.id"), F.col("departments.budget"))
    .orderBy("budget")
)
