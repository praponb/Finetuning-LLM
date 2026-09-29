from pyspark.sql import functions as F

result = (
    df_payments
    .select(
        "order_id",
        "status",
        F.date_trunc("WEEK", "paid_at").alias("paid_at_period"),
    )
)
