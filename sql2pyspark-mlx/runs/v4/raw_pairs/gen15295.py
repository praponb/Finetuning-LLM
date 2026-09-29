from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("job_title")
    .agg(F.min("hire_date").alias("min_hire_date"))
    .orderBy("job_title", F.col("min_hire_date").desc())
    .limit(10)
)
