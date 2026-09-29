from pyspark.sql import functions as F

result = (
    df_employees
    .select("id", "job_title", "salary", "level", F.trim("level").alias("clean_level"))
    .orderBy("salary")
)
