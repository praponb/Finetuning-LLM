from pyspark.sql import functions as F

result = (
    df_payments
    .select(
        "id",
        "amount",
        "status",
        F.when(F.col("status") == "REFUNDED", 1).otherwise(0).alias("is_refunded"),
    )
)
