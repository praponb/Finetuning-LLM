from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("method")
    .agg(
        F.sum("amount").alias("total_amount"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
        F.round(((100 * F.sum(F.when(F.col("method") == "bank_transfer", 1).otherwise(0))) / F.count("*")), 1).alias("pct_bank_transfer"),
    )
)
