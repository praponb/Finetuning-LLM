from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("d.id") == F.col("e.dept_id"), "left")
    .groupBy(F.col("d.id"), F.col("d.dept_name"))
    .agg(
        F.count(F.col("e.id")).alias("employee_count"),
        (F.max(F.col("e.salary")) - F.min(F.col("e.salary"))).alias("salary_range"),
        F.round(F.avg(F.col("e.salary")), 2).alias("avg_salary"),
    )
    .orderBy(F.col("avg_salary").desc(), "id", "dept_name")
    .limit(3)
)
