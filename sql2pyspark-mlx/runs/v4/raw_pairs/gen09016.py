from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .filter(F.col("e.hire_date") >= "2024-05-01")
    .groupBy(F.col("d.location"))
    .agg(F.min(F.col("e.salary")).alias("min_salary"))
)
