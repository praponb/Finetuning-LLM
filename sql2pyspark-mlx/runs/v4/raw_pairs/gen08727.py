from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(
        F.when(F.col("amount") >= 1630, "PREMIUM").otherwise("STANDARD").alias("amount_tier"),
    )
    .agg(
        F.percentile_approx("amount", [0.25, 0.5, 0.75]).alias("amount_quartiles"),
        F.count(F.when(F.col("method") == "bank_transfer", 1)).alias("bank_transfer_rows"),
        F.count(F.when(F.col("status") == "FAILED", 1)).alias("failed_rows"),
        F.sum("amount").alias("total_amount"),
        F.percentile_approx("amount", [0.25, 0.5, 0.75]).alias("amount_quartiles_2"),
    )
)
