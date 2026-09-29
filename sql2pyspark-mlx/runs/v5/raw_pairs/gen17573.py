from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method").isin(["cash", "bank_transfer"]) & (F.col("method") == "card"))
    .agg(
        F.sum("amount").alias("total_amount"),
        F.count(F.when(F.col("status") == "REFUNDED", 1)).alias("refunded_count"),
    )
)
