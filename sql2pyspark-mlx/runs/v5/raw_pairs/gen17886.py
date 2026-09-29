from pyspark.sql import functions as F

filtered_employees = (
    df_employees
    .filter((F.col("hire_date") >= "2025-03-17") & F.col("is_remote"))
    .withColumn("salary_scaled", F.round((F.col("salary") * 1.1), 2))
)

summary = (
    filtered_employees
    .groupBy("job_title")
    .agg(
        F.sum(F.when(F.col("job_title") == "Director", F.col("salary")).otherwise(0)).alias("director_salary"),
        F.sum(F.when(F.col("job_title") == "Analyst", 1).otherwise(0)).alias("analyst_count"),
    )
)

result = summary.select("job_title", "director_salary", "analyst_count")
