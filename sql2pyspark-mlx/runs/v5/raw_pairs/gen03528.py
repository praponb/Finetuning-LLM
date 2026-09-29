from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status").isin(["SUCCESS", "REFUNDED", "FAILED"]) & (F.col("paid_at") < "2024-04-25"))
    .select(
        "id",
        "method",
        F.concat_ws(",", "method", "status").alias("method_status_key"),
        F.round("amount", 0).alias("amount_rounded"),
    )
    .orderBy("method_status_key", F.col("amount_rounded").desc(), "id")
    .limit(20)
)
