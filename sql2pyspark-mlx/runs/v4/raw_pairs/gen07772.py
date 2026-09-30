from pyspark.sql import functions as F

result = (
    df_payments
    .select(
        "id",
        "paid_at",
        "amount",
        "order_id",
        F.col("amount").cast("int").alias("amount_int"),
        F.trim("status").alias("clean_status"),
    )
    .orderBy(F.col("order_id").desc(), "clean_status")
)
