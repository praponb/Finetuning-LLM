from pyspark.sql import functions as F

result = (
    df_employees.alias("employees")
    .join(df_departments.filter(F.col("budget") < 2539000).alias("departments"), F.col("employees.dept_id") == F.col("departments.id"), "left_semi")
    .select(F.col("employees.id"), F.col("employees.dept_id"))
)
