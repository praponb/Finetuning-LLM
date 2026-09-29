from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("paid_at") >= "2023-07-01") & (F.col("amount") < 1370))
    .select(
        "id",
        "amount",
        "order_id",
        F.datediff(F.current_date(), "paid_at").alias("days_since_paid_at"),
        F.length("method").alias("method_length"),
    )
)
