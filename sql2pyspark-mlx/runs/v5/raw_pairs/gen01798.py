from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("amount") >= 1550) & F.col("order_id").isNull() & (F.col("status") == "REFUNDED"))
    .groupBy(F.year("paid_at").alias("paid_year"))
    .agg(
        F.sum("amount").alias("total_amount"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )
    .orderBy(F.col("avg_amount").desc())
)
