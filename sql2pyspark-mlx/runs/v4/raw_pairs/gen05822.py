from pyspark.sql import functions as F

employees_by_department = (
    df_employees
    .groupBy("dept_id")
    .agg(F.sum("salary").alias("total_salary"))
)

result = (
    df_departments.alias("d")
    .join(employees_by_department.alias("ebd"), F.col("d.id") == F.col("ebd.dept_id"), "inner")
    .filter(F.col("ebd.total_salary") >= 533000)
    .select(F.col("d.dept_name"), F.col("ebd.total_salary"))
    .orderBy(F.col("total_salary").desc())
)
