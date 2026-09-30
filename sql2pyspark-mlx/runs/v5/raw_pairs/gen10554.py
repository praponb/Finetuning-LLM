from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.filter(F.col("hire_date").between("2023-03-01", "2023-03-05")).alias("e"), F.col("d.id") == F.col("e.dept_id"), "left_semi")
    .select(F.col("d.id"), F.col("d.budget"), F.col("d.dept_name"), F.col("d.location"))
)
