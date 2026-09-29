from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("d.id") == F.col("e.dept_id"), "inner")
    .filter(F.col("e.dept_id").isin([13, 18, 19]))
    .groupBy(F.col("d.id"), F.col("d.location"))
    .agg(F.count(F.col("e.id")).alias("employee_count"))
)
