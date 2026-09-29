from pyspark.sql import functions as F

cleaned = (
    df_payments
    .filter(F.col("paid_at") >= F.lit("2023-08-01").cast("date"))
    .select(
        "id",
        "order_id",
        "method",
        "status",
        F.round((F.col("amount") * 100), 2).alias("amount_adjusted"),
        F.coalesce("method", F.lit("DIRECT")).alias("resolved_method"),
    )
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("amount_adjusted") >= 67, "HIGH").otherwise("LOW").alias("amount_adjusted_tier"),
    )
    .agg(
        F.count_if(F.col("status") == "REFUNDED").alias("refunded_events"),
        F.count(F.when(F.col("method") == "promptpay", 1)).alias("promptpay_count"),
        F.percentile_approx("amount_adjusted", 0.9).alias("amount_adjusted_p90"),
    )
    .orderBy(F.col("amount_adjusted_p90").desc_nulls_first())
)
