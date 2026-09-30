from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.to_date("paid_at") >= "2025-01-01") | F.hour("paid_at").between(7, 14))
    .select(
        "id",
        "paid_at",
        "method",
        "amount",
        F.to_date("paid_at").alias("paid_at_date"),
    )
)
