from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method") == "bank_transfer")
    .select(
        "id",
        "order_id",
        F.when(F.col("amount") >= 1690, "High").when(F.col("amount") >= 700, "Medium").otherwise("Low").alias("amount_band"),
        F.date_trunc("YEAR", "paid_at").alias("paid_at_period"),
    )
    .orderBy("paid_at_period")
)
