from pyspark.sql import functions as F

employees_by_department = (
    df_employees
    .filter(F.col("dept_id") == 37)
    .groupBy("dept_id")
    .agg(F.min("hire_date").alias("min_hire_date"))
)

result = (
    df_departments.alias("d")
    .join(employees_by_department.alias("ebd"), F.col("d.id") == F.col("ebd.dept_id"), "left")
    .select(F.col("d.budget"), F.col("ebd.min_hire_date"))
    .orderBy("budget")
)
