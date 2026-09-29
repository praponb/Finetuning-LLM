from pyspark.sql import functions as F

base = (
    df_payments
    .filter((F.col("paid_at") >= F.lit("2023-10-25 12:00:00").cast("timestamp")) & (F.col("status") == "FAILED"))
    .withColumn("status_prefix", F.substring("status", 1, 3))
    .withColumn("method_clean", F.coalesce("method", F.lit("DIRECT")))
)

result = (
    base
    .groupBy(
        "method",
        F.when(F.col("amount") >= 1210, "PREMIUM").otherwise("STANDARD").alias("amount_tier"),
    )
    .agg(
        F.percentile_approx("amount", [0.25, 0.5, 0.75]).alias("amount_quartiles"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.count("*").alias("event_count"),
        ((F.unix_timestamp(F.max("paid_at")) - F.unix_timestamp(F.min("paid_at"))) / 60).alias("span_minutes"),
        F.stddev_samp("amount").alias("amount_stddev"),
    )
    .orderBy(F.col("avg_amount").desc())
)
