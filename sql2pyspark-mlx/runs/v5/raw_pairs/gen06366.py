from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.to_date("paid_at") >= "2023-05-27") & ((F.col("amount") < 1000) | (F.col("amount") <= 750)))
    .select(
        "id",
        "paid_at",
        "amount",
        F.date_trunc("MONTH", "paid_at").alias("paid_at_period"),
    )
)
