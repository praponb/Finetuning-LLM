from pyspark.sql import functions as F

result = (
    df_payments
    .select(
        "id",
        "method",
        "amount",
        F.length("method").alias("method_length"),
        F.date_format("paid_at", "yyyy-MM").alias("paid_at_label"),
    )
)
