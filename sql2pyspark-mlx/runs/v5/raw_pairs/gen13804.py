from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("d.id") == F.col("e.dept_id"), "left")
    .groupBy(F.col("d.dept_name"))
    .agg(F.min(F.col("e.hire_date")).alias("first_hire_date"))
)
