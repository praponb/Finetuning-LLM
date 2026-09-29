from pyspark.sql import functions as F

employees_by_department = (
    df_employees
    .groupBy("dept_id")
    .agg(F.avg("salary").alias("avg_salary"))
)

result = (
    df_departments.alias("d")
    .join(employees_by_department.alias("t"), F.col("d.id") == F.col("t.dept_id"), "left")
    .select(F.col("d.dept_name"), F.col("t.avg_salary"))
)
