from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("method")
    .agg(F.count(F.when(F.col("status") == "FAILED", 1)).alias("failed_count"))
    .orderBy(F.col("failed_count").desc())
)
