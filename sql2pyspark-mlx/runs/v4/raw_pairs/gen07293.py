from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("method")
    .agg(F.count("amount").alias("amount_count"))
    .filter(F.col("amount_count") >= 7)
    .orderBy(F.col("amount_count").desc(), "method")
    .limit(5)
)
