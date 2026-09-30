from pyspark.sql import functions as F

filtered_employees = (
    df_employees
    .filter(F.col("dept_id").isin([5, 6, 32]) | F.col("full_name").like("O%"))
    .withColumn("clean_full_name", F.trim("full_name"))
)

employees_stats = (
    filtered_employees
    .groupBy("is_remote")
    .agg(
        F.sum(F.when(F.col("job_title") == "Director", F.col("salary")).otherwise(0)).alias("director_salary"),
        F.max("hire_date").alias("latest_hire_date"),
    )
)

result = (
    employees_stats
    .filter(F.col("director_salary") >= 31)
    .orderBy(F.col("director_salary").desc(), "latest_hire_date", "is_remote")
    .limit(5)
)
