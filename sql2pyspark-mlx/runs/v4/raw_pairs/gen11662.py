from pyspark.sql import functions as F

result = (
    df_payments
    .filter(~F.col("status").isin(["SUCCESS", "REFUNDED"]) & F.col("status").isin(["REFUNDED", "SUCCESS", "FAILED"]) & ~F.col("status").isin(["FAILED", "REFUNDED", "SUCCESS"]))
    .select(
        "id",
        "amount",
        F.lower("method").alias("method_lower"),
        F.coalesce("amount", F.lit(0)).alias("amount_filled"),
    )
    .orderBy(F.col("amount").desc(), "id")
    .limit(10)
)
