from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("is_remote", "level", "job_title")
    .agg(F.min("hire_date").alias("min_hire_date"))
    .orderBy("min_hire_date")
)
