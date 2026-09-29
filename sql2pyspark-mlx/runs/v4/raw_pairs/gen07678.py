from pyspark.sql import functions as F

filtered_employees = df_employees.filter(F.col("dept_id").isin([17, 24]))

job_title_summary = (
    filtered_employees
    .groupBy("job_title", "level")
    .agg(F.avg("salary").alias("avg_salary"))
)

result = (
    job_title_summary
    .select("job_title", "level", "avg_salary")
    .orderBy("job_title", F.col("avg_salary").desc(), "level")
    .limit(10)
)
