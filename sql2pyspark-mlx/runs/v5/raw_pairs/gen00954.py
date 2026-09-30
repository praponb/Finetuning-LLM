from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("is_active")
    .agg(F.count(F.when(F.col("segment") == "SMB", 1)).alias("smb_count"))
    .orderBy("smb_count")
)
