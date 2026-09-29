from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("salary") > 101000)
    .select("level", F.col("is_remote").cast("int").alias("is_remote_flag"))
)
