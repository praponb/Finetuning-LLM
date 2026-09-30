from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .groupBy(F.col("d.dept_name"))
    .agg(F.sum(F.col("e.salary")).alias("total_salary"))
    .orderBy("dept_name")
)
