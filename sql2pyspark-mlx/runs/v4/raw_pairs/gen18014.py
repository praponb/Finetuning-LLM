from pyspark.sql import functions as F

result = (
    df_payments
    .select(
        "status",
        "method",
        "paid_at",
        F.col("amount").cast("int").alias("amount_int"),
    )
)
