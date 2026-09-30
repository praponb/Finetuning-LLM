from pyspark.sql import functions as F

result = (
    df_employees.alias("employees")
    .join(df_departments.filter(F.col("budget") < 2655000).alias("departments"), F.col("employees.dept_id") == F.col("departments.id"), "left_semi")
    .filter(F.col("employees.salary") >= 145000)
    .select(F.col("employees.id"), F.col("employees.manager_id"))
)
