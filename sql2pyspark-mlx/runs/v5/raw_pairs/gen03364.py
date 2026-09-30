from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("paid_at") < "2023-07-17") & F.col("order_id").isNull())
    .groupBy(
        "method",
        F.date_format("paid_at", "yyyy-MM").alias("paid_month"),
        F.year("paid_at").alias("paid_year"),
    )
    .agg(F.avg("amount").alias("avg_amount"))
    .orderBy("paid_month")
)
