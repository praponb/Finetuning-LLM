from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount") <= 1430)
    .select(
        "id",
        "status",
        "method",
        "paid_at",
        F.when(F.col("method") == "promptpay", 1).otherwise(0).alias("is_promptpay"),
    )
    .orderBy("method", "paid_at")
)
