from pyspark.sql import functions as F

employees_by_department = (
    df_employees
    .filter(F.col("manager_id").isin([10, 14]))
    .groupBy("dept_id")
    .agg(F.avg("salary").alias("avg_salary"))
)

result = (
    df_departments.alias("d")
    .join(employees_by_department.alias("ebd"), F.col("d.id") == F.col("ebd.dept_id"), "inner")
    .filter(F.col("ebd.avg_salary") >= 98000)
    .select(F.col("d.id"), F.col("d.budget"), F.col("ebd.avg_salary"))
    .orderBy(F.col("id").desc(), "avg_salary")
    .limit(10)
)
