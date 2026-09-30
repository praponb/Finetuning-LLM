from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("is_remote"))
    .select("job_title", F.col("is_remote").cast("int").alias("is_remote_flag"))
    .orderBy(F.col("job_title").desc(), F.col("is_remote_flag").desc())
)
