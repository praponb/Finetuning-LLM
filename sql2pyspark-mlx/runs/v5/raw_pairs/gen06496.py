from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .groupBy(F.col("d.location"))
    .agg(
        F.min(F.col("e.hire_date")).alias("first_hire_date"),
        F.avg(F.col("e.salary")).alias("avg_salary"),
    )
    .orderBy("location", "avg_salary")
)
