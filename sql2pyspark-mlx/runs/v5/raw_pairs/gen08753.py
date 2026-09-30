from pyspark.sql import functions as F

result = (
    df_payments
    .filter(~F.col("method").isin(["promptpay", "cash"]) & ~F.col("status").isin(["REFUNDED", "FAILED"]))
    .select(
        "id",
        "paid_at",
        "method",
        "status",
        F.datediff(F.current_date(), "paid_at").alias("days_since_paid_at"),
    )
)
