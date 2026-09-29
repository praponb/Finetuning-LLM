from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount").between(800, 1110))
    .select(
        "id",
        "method",
        "status",
        F.when(F.col("status") == "REFUNDED", 1).otherwise(0).alias("is_refunded"),
    )
)
