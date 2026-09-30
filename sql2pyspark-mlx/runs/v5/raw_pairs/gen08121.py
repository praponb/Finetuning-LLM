from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("hire_date") >= "2024-09-10") & F.col("is_remote"))
    .groupBy("job_title", F.date_trunc("MONTH", "hire_date").alias("hire_month"))
    .agg(
        F.count(F.when(F.col("level") == "L3", 1)).alias("l3_count"),
        F.count(F.when(F.col("job_title") == "Director", 1)).alias("director_count"),
    )
    .orderBy("job_title")
)
