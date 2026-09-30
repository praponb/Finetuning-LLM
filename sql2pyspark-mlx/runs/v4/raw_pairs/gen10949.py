from pyspark.sql import functions as F

employees_by_department = (
    df_employees
    .filter(F.month("hire_date").isin([1, 9, 11]))
    .groupBy("dept_id")
    .agg(F.round(F.avg("salary"), 2).alias("avg_salary"))
)

result = (
    df_departments.alias("d")
    .join(employees_by_department.alias("t"), F.col("d.id") == F.col("t.dept_id"), "left")
    .select(F.col("d.location"), F.col("t.avg_salary"))
)
