from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status").isin(["REFUNDED", "SUCCESS", "FAILED"]))
    .select(
        "id",
        "amount",
        "order_id",
        F.when(F.col("amount") >= 1640, "High").when(F.col("amount") >= 750, "Medium").otherwise("Low").alias("amount_tier"),
    )
)
