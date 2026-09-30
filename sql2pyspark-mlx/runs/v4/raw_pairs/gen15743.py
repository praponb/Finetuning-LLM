from pyspark.sql import functions as F

department_stats = (
    df_employees
    .filter(F.col("manager_id").isin([20, 30, 40]))
    .groupBy("dept_id")
    .agg(F.min("hire_date").alias("min_hire_date"))
)

result = (
    df_departments.alias("d")
    .join(department_stats.alias("t"), F.col("d.id") == F.col("t.dept_id"), "left")
    .select(F.col("d.id"), F.col("d.dept_name"), F.col("t.min_hire_date"))
)
