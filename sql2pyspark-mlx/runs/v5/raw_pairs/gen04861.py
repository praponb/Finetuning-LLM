from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("method")
    .agg(
        F.count(F.when(F.col("method") == "bank_transfer", 1)).alias("bank_transfer_count"),
        F.count(F.when(F.col("status") == "REFUNDED", 1)).alias("refunded_count"),
    )
    .orderBy("method")
)
