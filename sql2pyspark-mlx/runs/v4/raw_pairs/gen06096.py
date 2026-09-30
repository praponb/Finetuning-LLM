from pyspark.sql import functions as F

valid = (
    df_payments
    .filter((F.col("paid_at") >= F.lit("2024-01-13").cast("date")) & F.col("order_id").isin([1, 29, 35]))
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        F.when(F.col("amount") >= 1320, "High").when(F.col("amount") >= 240, "Medium").otherwise("Low").alias("amount_band"),
    )
)

result = (
    valid
    .groupBy("order_id")
    .pivot("method", ["promptpay", "bank_transfer"])
    .agg(F.avg("amount").alias("total"), F.count(F.lit(1)).alias("item_count"))
)
