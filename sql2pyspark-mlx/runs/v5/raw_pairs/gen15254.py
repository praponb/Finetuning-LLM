from pyspark.sql import functions as F

department_stats = (
    df_employees
    .groupBy("dept_id")
    .agg(F.min("hire_date").alias("first_hire_date"))
)

result = (
    df_departments.alias("d")
    .join(department_stats.alias("ds"), F.col("d.id") == F.col("ds.dept_id"), "left")
    .select(F.col("d.id"), F.col("d.dept_name"), F.col("ds.first_hire_date"))
    .orderBy("dept_name")
)
