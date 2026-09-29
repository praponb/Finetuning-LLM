from pyspark.sql import functions as F

base = (
    df_payments
    .filter((F.col("paid_at") > F.lit("2024-01-30").cast("date")) & F.col("order_id").isNotNull())
    .select(
        "id",
        "method",
        "amount",
        "paid_at",
        F.trim("status").alias("clean_status"),
        F.coalesce("method", F.lit("DIRECT")).alias("method_clean"),
    )
)

result = (
    base
    .groupBy(F.date_trunc("month", "paid_at").alias("activity_month"))
    .agg(
        F.percentile_approx("amount", [0.25, 0.5, 0.75]).alias("amount_quartiles"),
        F.count_if(F.col("method") == "card").alias("card_events"),
        ((F.unix_timestamp(F.max("paid_at")) - F.unix_timestamp(F.min("paid_at"))) / 60).alias("duration_min"),
        F.count("*").alias("num_rows"),
        ((F.unix_timestamp(F.max("paid_at")) - F.unix_timestamp(F.min("paid_at"))) / 60).alias("span_minutes"),
    )
)
