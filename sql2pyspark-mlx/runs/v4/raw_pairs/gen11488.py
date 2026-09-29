from pyspark.sql import functions as F

result = (
    df_payments
    .select(
        "id",
        "order_id",
        "paid_at",
        "status",
        F.date_format("paid_at", "yyyy-MM").alias("paid_at_label"),
        F.when(F.col("amount") >= 1480, "High").when(F.col("amount") >= 390, "Medium").otherwise("Low").alias("amount_band"),
    )
    .orderBy("amount_band", "id")
    .limit(3)
)
