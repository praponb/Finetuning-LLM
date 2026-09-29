from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status").isin(["REFUNDED", "FAILED", "SUCCESS"]) & ((F.col("order_id") == 33) | F.col("amount").isNotNull()))
    .select(
        "id",
        "status",
        "method",
        F.round((F.col("amount") * 100), 2).alias("amount_scaled"),
    )
)
