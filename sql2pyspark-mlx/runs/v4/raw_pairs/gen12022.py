from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("segment", F.quarter("signup_date").alias("signup_quarter"))
    .agg(F.sum(F.when(F.col("segment") == "SMB", 1).otherwise(0)).alias("smb_count"))
    .filter(F.col("smb_count") > 42)
    .orderBy("segment")
)
