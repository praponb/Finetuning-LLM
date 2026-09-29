from pyspark.sql import functions as F

department_stats = (
    df_employees
    .filter(F.col("salary") > 94000)
    .groupBy("dept_id")
    .agg(F.count(F.when(F.col("level") == "L4", 1)).alias("l4_count"))
)

result = (
    df_departments.alias("d")
    .join(department_stats.alias("ds"), F.col("d.id") == F.col("ds.dept_id"), "inner")
    .select(F.col("d.dept_name"), F.col("ds.l4_count"))
)
