from pyspark.sql import functions as F

employees_by_department = (
    df_employees
    .groupBy("dept_id")
    .agg(F.max("salary").alias("max_salary"))
)

result = (
    df_departments.alias("d")
    .join(employees_by_department.alias("ebd"), F.col("d.id") == F.col("ebd.dept_id"), "inner")
    .filter(F.col("ebd.max_salary") > 121000)
    .select(F.col("d.dept_name"), F.col("d.location"), F.col("ebd.max_salary"))
)
