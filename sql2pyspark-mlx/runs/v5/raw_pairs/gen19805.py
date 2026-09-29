from pyspark.sql import functions as F

employees_by_department = (
    df_employees
    .filter(F.length("email") < 7)
    .groupBy("dept_id")
    .agg(F.round(F.avg("salary"), 2).alias("avg_salary"))
)

result = (
    df_departments.alias("d")
    .join(employees_by_department.alias("ebd"), F.col("d.id") == F.col("ebd.dept_id"), "left")
    .filter(F.col("ebd.avg_salary") >= 55)
    .select(F.col("d.id"), F.col("d.dept_name"), F.col("ebd.avg_salary"))
    .orderBy(F.col("avg_salary").desc(), "dept_name")
)
