from pyspark.sql import functions as F

result = (
    df_employees
    .select("id", "job_title", F.trim("level").alias("clean_level"))
    .orderBy("job_title", "id")
    .limit(10)
)
