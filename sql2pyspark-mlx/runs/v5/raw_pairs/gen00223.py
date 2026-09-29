from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("d.id") == F.col("e.dept_id"), "inner")
    .groupBy(F.col("d.location"))
    .agg(
        F.max(F.col("e.hire_date")).alias("last_hire_date"),
        F.avg(F.col("e.salary")).alias("avg_salary"),
    )
)
