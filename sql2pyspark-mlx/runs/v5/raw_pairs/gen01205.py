from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("end_date").desc_nulls_last(), "id")
w2 = Window.partitionBy("customer_id")
w3 = Window.partitionBy("customer_id", "plan")
w4 = Window.partitionBy("status")
w5 = Window.orderBy(F.col("monthly_fee").desc())
w6 = Window.partitionBy("status").orderBy(F.col("customer_id_min_status_stddev_samp_monthly_fee_filled").asc_nulls_first())

base = (
    df_subscriptions
    .filter((F.col("start_date") > F.lit("2023-04-16").cast("date")) & F.col("customer_id").isNotNull())
    .select("*", F.date_add("end_date", 66).alias("end_date_plus_66d"))
)

latest = (
    base
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

remaining_keys = (
    latest
    .select("status")
    .subtract(
        df_subscriptions
        .filter(F.col("start_date") <= "2023-06-01")
        .select("status")
    )
)

verified = (
    latest
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

with_windows = (
    verified
    .select(
        "*",
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w2)), 4).alias("monthly_fee_share"),
        F.max("monthly_fee").over(w3).alias("customer_id_max_monthly_fee"),
        F.stddev_samp("monthly_fee").over(w4).alias("status_stddev_samp_monthly_fee"),
    )
)

enriched = (
    with_windows
    .select(
        "end_date",
        "status",
        "end_date_plus_66d",
        "rank_in_group",
        F.dense_rank().over(w5).alias("monthly_fee_dense_rank"),
        F.sum("monthly_fee").over(w4).alias("status_sum_monthly_fee"),
        F.min("status_stddev_samp_monthly_fee").over(w2).alias("customer_id_min_status_stddev_samp_monthly_fee"),
    )
)

base_subscriptions = (
    enriched
    .filter(F.col("customer_id_min_status_stddev_samp_monthly_fee") >= 186)
    .select(
        "end_date",
        "status",
        "end_date_plus_66d",
        "rank_in_group",
        "monthly_fee_dense_rank",
        "status_sum_monthly_fee",
        "customer_id_min_status_stddev_samp_monthly_fee",
        F.coalesce("customer_id_min_status_stddev_samp_monthly_fee", F.lit(0)).alias("customer_id_min_status_stddev_samp_monthly_fee_filled"),
    )
)

enriched_subscriptions = (
    base_subscriptions
    .withColumn("status_count_customer_id_min_status_stddev_samp_monthly_fee_filled", F.count("customer_id_min_status_stddev_samp_monthly_fee_filled").over(w4))
    .withColumn("customer_id_min_status_stddev_samp_monthly_fee_filled_percent_rank", F.percent_rank().over(w6))
    .withColumn("customer_id_min_status_stddev_samp_monthly_fee_share", F.round((F.col("customer_id_min_status_stddev_samp_monthly_fee") / F.sum("customer_id_min_status_stddev_samp_monthly_fee").over(w4)), 4))
)

daily = (
    enriched_subscriptions
    .groupBy("status")
    .agg(
        F.count(F.when(F.col("status") == "EXPIRED", 1)).alias("expired_rows"),
        F.count(F.when(F.col("status") == "PAUSED", 1)).alias("paused_rows"),
    )
)

result = (
    daily
    .select(
        "status",
        "expired_rows",
        "paused_rows",
        F.when(F.col("status").rlike("^(ACTIVE|CANCELLED)$"), "PRIORITY").otherwise("GROUP_B").alias("status_zone"),
    )
    .orderBy("status_zone", "status")
)
