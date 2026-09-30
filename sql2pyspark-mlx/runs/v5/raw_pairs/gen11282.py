from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("method")
    .agg(
        F.sum("amount").alias("total_amount"),
        F.count("*").alias("payment_count"),
    )
    .filter(F.col("total_amount") > 9000)
    .select("method", "total_amount", "payment_count")
    .orderBy(F.col("total_amount").desc(), "method")
)
