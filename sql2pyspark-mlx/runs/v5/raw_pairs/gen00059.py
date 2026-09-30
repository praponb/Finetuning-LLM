from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id")
w2 = Window.orderBy(F.col("monthly_fee").desc_nulls_first())
w_hist = Window.orderBy(F.col("monthly_fee").asc_nulls_last())

base = (
    df_subscriptions
    .filter((F.col("end_date") > F.lit("2023-09-09").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "*",
        F.when(F.col("monthly_fee") >= 188, "High").when(F.col("monthly_fee") >= 41, "Medium").otherwise("Low").alias("monthly_fee_tier"),
    )
)

valid = (
    base
    .filter((F.col("monthly_fee") < 120) & (F.col("end_date") >= F.lit("2023-10-01").cast("date")))
    .select(
        "id",
        "customer_id",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        "monthly_fee_tier",
        F.upper("status").alias("status_upper"),
    )
)

remaining_keys = (
    valid
    .select("customer_id", "id")
    .exceptAll(
        df_subscriptions
        .filter(F.col("customer_id").isNull())
        .select("customer_id", "id")
    )
)

verified = (
    valid
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

metrics = (
    verified
    .filter(F.col("start_date").between("2024-02-01", "2025-06-13"))
    .select(
        "*",
        F.stddev_samp("monthly_fee").over(w).alias("customer_id_stddev_samp_monthly_fee"),
        F.dense_rank().over(w2).alias("monthly_fee_dense_rank"),
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w)), 4).alias("monthly_fee_share"),
        F.percent_rank().over(w_hist).alias("monthly_fee_percent_rank"),
    )
)

summary = (
    metrics
    .groupBy(
        F.when(F.col("monthly_fee") >= 186, "PREMIUM").otherwise("STANDARD").alias("monthly_fee_tier"),
        F.date_trunc("week", "end_date").alias("week_start"),
    )
    .agg(
        F.count_if(F.col("status") == "EXPIRED").alias("expired_events"),
        F.count(F.when(F.col("status") == "CANCELLED", 1)).alias("cancelled_count"),
    )
)

result = (
    summary
    .select("monthly_fee_tier", "week_start", "cancelled_count")
    .orderBy("monthly_fee_tier")
)
