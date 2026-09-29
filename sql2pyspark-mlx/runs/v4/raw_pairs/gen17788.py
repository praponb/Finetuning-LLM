from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isNull() & F.col("method").isin(["bank_transfer", "cash"]))
    .groupBy("method")
    .agg(
        F.min("amount").alias("min_amount"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
        F.max("paid_at").alias("last_paid_at"),
    )
    .filter(F.col("min_amount") < 640)
    .orderBy(F.col("amount_range").desc())
)
