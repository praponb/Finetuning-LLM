from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.filter(F.col("budget").between(1802000, 3891000)).alias("d"), F.col("e.dept_id") == F.col("d.id"), "left_anti")
    .filter(F.col("e.dept_id").isin([10, 15, 22]))
    .select(F.col("e.id"), F.col("e.hire_date"), F.col("e.full_name"))
    .orderBy("full_name")
)
