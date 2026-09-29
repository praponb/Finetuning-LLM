from pyspark.sql import functions as F

department_totals = (
    df_employees
    .filter(F.col("full_name").like("Em%"))
    .groupBy("dept_id")
    .agg(
        F.sum(F.when(F.col("job_title") == "Director", 1).otherwise(0)).alias("director_count"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_totals.alias("t"), F.col("d.id") == F.col("t.dept_id"), "left")
    .filter(F.col("t.director_count") >= 39)
    .select(F.col("d.id"), F.col("d.location"), F.col("t.director_count"))
    .orderBy("id")
)
