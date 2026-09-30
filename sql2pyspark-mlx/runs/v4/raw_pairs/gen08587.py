from pyspark.sql import functions as F

department_totals = (
    df_employees
    .groupBy("dept_id")
    .agg(F.count("*").alias("total_count"))
)

result = (
    df_departments.alias("d")
    .join(department_totals.alias("t"), F.col("d.id") == F.col("t.dept_id"), "left")
    .filter(F.col("t.total_count") >= 5)
    .select(F.col("d.id"), F.col("d.location"), F.col("t.total_count"))
)
