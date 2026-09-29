from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("d.id") == F.col("e.dept_id"), "inner")
    .groupBy(F.col("d.location"))
    .agg(
        F.count(F.col("e.id")).alias("employee_count"),
        F.count(F.when(F.col("e.job_title") == "Director", 1)).alias("director_count"),
    )
)
