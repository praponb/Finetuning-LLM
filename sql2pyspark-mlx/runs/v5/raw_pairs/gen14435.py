from pyspark.sql import functions as F

result = (
    df_employees
    .select("manager_id", "job_title")
    .orderBy(F.col("job_title").desc())
)
