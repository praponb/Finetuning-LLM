from pyspark.sql import functions as F

recent_payments = (
    df_payments
    .filter(F.col("order_id").isin([8, 22, 24]) | F.col("status").isin(["SUCCESS", "REFUNDED", "FAILED"]))
)

agg = (
    recent_payments
    .groupBy("status")
    .agg(
        F.min("paid_at").alias("first_paid_at"),
        F.count("*").alias("row_count"),
    )
)

result = (
    agg
    .filter(F.col("row_count") > 10)
    .select("status", "row_count")
    .orderBy("row_count")
)
