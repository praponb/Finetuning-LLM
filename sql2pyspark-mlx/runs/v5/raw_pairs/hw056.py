from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("dept_id")
    .agg(F.sum(F.when(F.col("is_remote"), 1).otherwise(0)).alias("remote_count"))
)
