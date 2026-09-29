from pyspark.sql import functions as F

result = (
    df_employees
    .agg(F.sum(F.when(F.col("level") == "L2", 1).otherwise(0)).alias("l2_count"))
    .orderBy(F.col("l2_count").desc())
)
