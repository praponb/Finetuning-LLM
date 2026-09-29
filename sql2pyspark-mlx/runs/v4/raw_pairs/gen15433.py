from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("d.id") == F.col("e.dept_id"), "left")
    .filter(F.col("e.salary") < 116000)
    .groupBy(F.col("d.id"), F.col("d.dept_name"))
    .agg(F.count(F.col("e.id")).alias("employee_count"))
    .orderBy(F.col("id").desc(), "dept_name")
    .limit(5)
)
