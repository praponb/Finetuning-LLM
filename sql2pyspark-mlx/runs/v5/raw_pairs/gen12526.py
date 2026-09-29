from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("method", "status")
    .agg(F.count("*").alias("row_count"))
    .orderBy(F.col("method").desc(), "row_count")
)
