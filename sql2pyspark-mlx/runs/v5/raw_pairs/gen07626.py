from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.hour("paid_at").between(8, 14))
    .select(
        "id",
        "method",
        "paid_at",
        "amount",
        F.date_format("paid_at", "MMM yyyy").alias("paid_at_label"),
    )
    .orderBy("id")
)
