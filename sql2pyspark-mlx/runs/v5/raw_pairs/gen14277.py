from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .filter(F.col("e.hire_date") < "2023-12-04")
    .select(F.col("e.id"), F.col("e.salary"), F.col("e.dept_id"), F.col("d.budget"))
)
