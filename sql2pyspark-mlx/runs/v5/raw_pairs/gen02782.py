from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "left")
    .groupBy(
        F.col("d.dept_name"),
        F.when(F.col("d.location").isin(["Berlin", "Tokyo"]), "priority").otherwise("other").alias("location_group"),
    )
    .agg(F.count(F.when(F.col("e.job_title") == "Director", 1)).alias("director_count"))
)
