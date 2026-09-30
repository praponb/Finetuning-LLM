from pyspark.sql import functions as F

result = (
    df_employees
    .select("id", "dept_id", F.col("is_remote").cast("int").alias("is_remote_flag"))
)
