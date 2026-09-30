from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status").isin(["SUCCESS", "FAILED", "REFUNDED"]))
    .select(
        "id",
        "status",
        "method",
        F.when(F.col("status") == "REFUNDED", 1).otherwise(0).alias("is_refunded"),
        F.round((F.col("amount") * 0.85), 2).alias("amount_adjusted"),
    )
    .orderBy("status")
)
