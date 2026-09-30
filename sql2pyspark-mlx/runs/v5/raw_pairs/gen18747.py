from pyspark.sql import functions as F

cleaned = (
    df_payments
    .filter(F.col("method").isNotNull())
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        F.length("method").alias("method_length"),
        F.coalesce("method", F.lit("NONE")).alias("method_clean"),
    )
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("amount") >= 1210, "HIGH").otherwise("LOW").alias("amount_tier"),
        "order_id",
        F.date_trunc("month", "paid_at").alias("activity_month"),
    )
    .agg(
        F.percentile_approx("amount", 0.95).alias("amount_p95"),
        F.count(F.when(F.col("method") == "promptpay", 1)).alias("promptpay_count"),
    )
    .filter(F.col("promptpay_count") > 1)
    .orderBy(F.col("promptpay_count").desc_nulls_first())
)
