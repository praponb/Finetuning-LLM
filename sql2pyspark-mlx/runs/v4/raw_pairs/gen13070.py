from pyspark.sql import functions as F

result = (
    df_payments
    .filter(~(F.col("status").isin(["FAILED", "REFUNDED"]) & (F.col("amount") > 510) & (F.col("order_id") == 24)))
    .select(
        "paid_at",
        "status",
        "amount",
        F.col("amount").cast("int").alias("amount_int"),
    )
)
