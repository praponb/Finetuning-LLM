from pyspark.sql import functions as F

result = (
    df_employees
    .filter(~F.col("full_name").like("%Ford%") & (F.col("job_title") == "Designer"))
    .select("id", "is_remote", "level", F.length("email").alias("email_length"))
)
