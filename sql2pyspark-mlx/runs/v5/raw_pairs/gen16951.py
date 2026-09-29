from pyspark.sql import functions as F

department_stats = (
    df_employees
    .groupBy("dept_id")
    .agg(F.sum("salary").alias("total_salary"))
)

result = (
    df_departments.alias("d")
    .join(department_stats.alias("ds"), F.col("d.id") == F.col("ds.dept_id"), "left")
    .filter(F.col("ds.total_salary") > 551000)
    .select(F.col("d.dept_name"), F.col("ds.total_salary"))
)
