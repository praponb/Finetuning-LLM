from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("monthly_fee").desc_nulls_last())
w2 = Window.partitionBy("status")
w3 = Window.partitionBy("status").orderBy(F.col("end_date_period").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w4 = Window.orderBy(F.col("monthly_fee_int").desc_nulls_first())
w5 = Window.orderBy(F.col("end_date_period").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w6 = Window.orderBy(F.col("monthly_fee_int_max_90d").asc_nulls_first())
w7 = Window.orderBy(F.col("monthly_fee_int_max_90d").desc())
w8 = Window.orderBy("monthly_fee_int_percent_rank_times_monthly_fee_int")
w9 = Window.orderBy(F.col("monthly_fee_int_max_90d_dense_rank_2").desc())

matched_keys = (
    df_subscriptions
    .select("plan", "id")
    .intersect(
        df_subscriptions
        .filter(F.col("end_date") <= "2024-12-01")
        .select("plan", "id")
    )
)

verified = (
    df_subscriptions
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.plan") == F.col("k.plan")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

filtered = (
    verified
    .filter((F.col("end_date") >= F.to_date(F.lit("2023-09-24"))) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "end_date",
        "monthly_fee",
        "status",
        F.col("monthly_fee").cast("int").alias("monthly_fee_int"),
        F.date_trunc("week", "end_date").alias("end_date_period"),
    )
)

scored = (
    filtered
    .select(
        "*",
        F.rank().over(w).alias("monthly_fee_rank"),
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w2)), 4).alias("monthly_fee_share"),
        F.max("monthly_fee_int").over(w3).alias("monthly_fee_int_max_90d"),
    )
)

valid = (
    scored
    .filter(F.col("status") == "CANCELLED")
    .select(
        "id",
        "end_date",
        "monthly_fee_int",
        "end_date_period",
        "monthly_fee_share",
        "monthly_fee_int_max_90d",
        F.substring("status", 1, 4).alias("status_prefix"),
    )
)

with_windows = (
    valid
    .select(
        "*",
        F.percent_rank().over(w4).alias("monthly_fee_int_percent_rank"),
        F.avg("monthly_fee_int_max_90d").over(w5).alias("monthly_fee_int_max_90d_avg_30d"),
        F.dense_rank().over(w6).alias("monthly_fee_int_max_90d_dense_rank"),
        F.avg("monthly_fee_share").over(w5).alias("monthly_fee_share_avg_30d"),
        F.dense_rank().over(w7).alias("monthly_fee_int_max_90d_dense_rank_2"),
    )
)

valid_subscriptions = (
    with_windows
    .filter((F.col("end_date") >= F.to_date(F.lit("2023-09-07"))) & (F.col("monthly_fee_int_max_90d") >= 44))
    .select(
        "*",
        (F.col("monthly_fee_int_percent_rank") * F.col("monthly_fee_int")).alias("monthly_fee_int_percent_rank_times_monthly_fee_int"),
    )
)

enriched = (
    valid_subscriptions
    .select(
        "*",
        F.cume_dist().over(w8).alias("monthly_fee_int_percent_rank_times_monthly_fee_int_cume_dist"),
        F.cume_dist().over(w9).alias("monthly_fee_int_max_90d_dense_rank_2_cume_dist"),
    )
)

result = (
    enriched
    .groupBy(
        F.date_trunc("QUARTER", "end_date").alias("quarter_start"),
        F.when(F.col("monthly_fee_int_max_90d") >= 90, "HIGH").otherwise("LOW").alias("monthly_fee_int_max_90d_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.percentile_approx("monthly_fee_int_max_90d_dense_rank_2_cume_dist", [0.25, 0.5, 0.75]).alias("monthly_fee_int_max_90d_dense_rank_2_cume_dist_quartiles"),
        ((F.unix_timestamp(F.max("end_date_period")) - F.unix_timestamp(F.min("end_date_period"))) / 60).alias("span_minutes"),
        F.percentile_approx("monthly_fee_int_max_90d_dense_rank_2", 0.9).alias("monthly_fee_int_max_90d_dense_rank_2_p90"),
        F.round(F.avg("monthly_fee_int_max_90d"), 2).alias("avg_monthly_fee_int_max_90d"),
    )
)
