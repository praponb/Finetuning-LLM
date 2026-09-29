from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("city")
    .pivot("segment", ["Consumer", "SMB"])
    .agg(F.sum("credit_limit").alias("amount"))
    .select("city", F.col("Consumer").alias("consumer"), F.col("SMB").alias("smb"))
)
