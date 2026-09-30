from pyspark.sql import functions as F

result = (
    df_employees
    .select("id", "job_title", "manager_id", F.length("level").alias("level_length"))
)
