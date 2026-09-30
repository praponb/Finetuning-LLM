from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("d.id") == F.col("e.dept_id"), "left")
    .filter(F.col("d.budget").between(3092000, 3914000))
    .groupBy(F.col("d.id"))
    .agg(
        F.count(F.col("e.id")).alias("employee_count"),
        F.avg(F.col("e.salary")).alias("avg_salary"),
    )
    .orderBy("avg_salary")
)
