from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")
w2 = Window.partitionBy("method").orderBy(F.col("paid_at_period").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w3 = Window.partitionBy("method", "status")
w4 = Window.partitionBy("status").orderBy(F.col("paid_at_period").cast("bigint")).rangeBetween(-2592000, Window.currentRow)

cleaned = (
    df_payments
    .filter(F.col("paid_at") >= F.lit("2023-06-04 08:00:00").cast("timestamp"))
    .select(
        "id",
        "method",
        "status",
        F.date_trunc("MONTH", "paid_at").alias("paid_at_period"),
    )
)

matched_keys = (
    cleaned
    .select("method", "id")
    .intersectAll(
        df_payments
        .filter(F.col("order_id").isNull())
        .select("method", "id")
    )
)

verified = (
    cleaned
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.method") == F.col("k.method")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

base = (
    verified
    .filter(F.col("method").isNotNull())
    .withColumn("method_prefix", F.substring("method", 1, 3))
    .withColumn("days_since_paid_at_period", F.datediff(F.current_date(), "paid_at_period"))
    .withColumn("method_filled", F.coalesce("method", F.lit("N/A")))
)

remaining_keys = (
    base
    .select("status", "method")
    .subtract(
        df_payments
        .filter(F.col("amount").between(640, 1460))
        .select("status", "method")
    )
)

verified_payments = (
    base
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.method") == F.col("k.method")), "inner")
    .select("s.*")
)

metrics = (
    verified_payments
    .withColumn("status_stddev_samp_days_since_paid_at_period", F.stddev_samp("days_since_paid_at_period").over(w))
    .withColumn("days_since_paid_at_period_count_90d", F.count("days_since_paid_at_period").over(w2))
)

with_windows = (
    metrics
    .select(
        "*",
        F.max("days_since_paid_at_period_count_90d").over(w3).alias("method_max_days_since_paid_at_period_count_90d"),
        F.count("days_since_paid_at_period").over(w4).alias("days_since_paid_at_period_count_30d"),
        F.round((F.col("days_since_paid_at_period_count_90d") / F.sum("days_since_paid_at_period_count_90d").over(w)), 4).alias("days_since_paid_at_period_count_90d_share"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "status",
        "paid_at_period",
        "method_prefix",
        "status_stddev_samp_days_since_paid_at_period",
        "method_max_days_since_paid_at_period_count_90d",
        "days_since_paid_at_period_count_90d_share",
    )
    .orderBy(F.col("paid_at_period").desc(), "status")
)
