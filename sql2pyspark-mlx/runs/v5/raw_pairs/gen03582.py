from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method").isNotNull())
    .select(
        "id",
        "paid_at",
        "method",
        F.date_format("paid_at", "MMM yyyy").alias("paid_at_label"),
    )
    .orderBy("paid_at")
)
