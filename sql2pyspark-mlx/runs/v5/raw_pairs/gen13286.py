from pyspark.sql import functions as F

employees_by_department = (
    df_employees
    .groupBy("dept_id")
    .agg(F.count("*").alias("cnt"))
)

result = (
    df_departments.alias("d")
    .join(employees_by_department.alias("ebd"), F.col("d.id") == F.col("ebd.dept_id"), "left")
    .filter(F.col("ebd.cnt") > 4)
    .select(F.col("d.dept_name"), F.col("d.budget"), F.col("ebd.cnt"))
)
