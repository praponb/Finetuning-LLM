from pyspark.sql import functions as F

filtered_employees = (
    df_employees
    .filter(F.col("job_title").isin(["Engineer", "Analyst"]) & (F.col("level").isNull() | F.col("dept_id").isin([15, 27, 33])))
    .withColumn("salary_whole", F.floor("salary"))
)

job_title_summary = (
    filtered_employees
    .groupBy("job_title")
    .agg(
        (F.max("salary_whole") - F.min("salary_whole")).alias("salary_whole_range"),
        F.avg("salary").alias("avg_salary"),
    )
)

result = job_title_summary.select("job_title", "salary_whole_range")
