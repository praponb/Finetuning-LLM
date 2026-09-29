from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("job_title") == "Designer") | F.col("job_title").isin(["Engineer", "Manager", "Director"]))
    .select("id", "is_remote", F.col("is_remote").cast("int").alias("is_remote_flag"))
)
