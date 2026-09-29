from pyspark.sql import functions as F

result = (
    df_payments
    .filter(~F.col("status").isin(["FAILED", "REFUNDED"]))
    .cube(
        "status",
        F.when(F.col("amount") >= 1390, "high").when(F.col("amount") >= 330, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("status") == "FAILED", F.col("amount")).otherwise(0)).alias("failed_amount"),
        F.min("paid_at").alias("min_paid_at"),
        F.count("amount").alias("amount_count"),
    )
)
