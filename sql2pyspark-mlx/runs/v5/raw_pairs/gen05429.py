from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.filter(F.year("hire_date") == 2025).alias("e"), F.col("d.id") == F.col("e.dept_id"), "left_semi")
    .select(F.col("d.id"), F.col("d.location"))
)
