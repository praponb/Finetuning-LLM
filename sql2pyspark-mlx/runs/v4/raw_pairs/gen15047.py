from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.length("email") >= 8) & (F.col("segment") == "SMB"))
    .groupBy("segment")
    .agg(F.sum(F.when(F.col("country") == "DE", 1).otherwise(0)).alias("de_count"))
    .filter(F.col("de_count") > 60)
)
