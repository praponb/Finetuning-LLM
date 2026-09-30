from pyspark.sql import functions as F

department_stats = (
    df_employees
    .groupBy("dept_id")
    .agg(F.sum("salary").alias("total_salary"))
)

result = (
    df_departments.alias("d")
    .join(department_stats.alias("t"), F.col("d.id") == F.col("t.dept_id"), "inner")
    .filter(F.col("t.total_salary") > 505000)
    .select(F.col("d.id"), F.col("d.dept_name"), F.col("t.total_salary"))
)
