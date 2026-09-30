from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(
        F.date_trunc("QUARTER", "paid_at").alias("quarter_start"),
        F.date_trunc("MONTH", "paid_at").alias("activity_month"),
        F.date_trunc("WEEK", "paid_at").alias("week_start"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.percentile_approx("amount", 0.95).alias("amount_p95"),
        F.stddev_samp("amount").alias("amount_stddev"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.min("paid_at").alias("first_paid_at"),
        ((F.unix_timestamp(F.max("paid_at")) - F.unix_timestamp(F.min("paid_at"))) / 60).alias("span_minutes"),
    )
)
