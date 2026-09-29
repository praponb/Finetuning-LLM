from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy(F.col("subscription_id").asc_nulls_first())
w2 = Window.orderBy("subscription_id", "id", "subscription_id").rowsBetween(-5, Window.currentRow)
w_ordered = Window.partitionBy("country").orderBy(F.col("started_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w4 = Window.partitionBy("device")
w5 = Window.orderBy("subscription_id_sum_7d", "id", "subscription_id").rowsBetween(-2, -1)
w6 = Window.partitionBy(F.col("s.id"), F.col("s.subscription_id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())
w7 = Window.partitionBy("web_event_page", "device").orderBy("monthly_fee_avg_2_prior", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("country").orderBy("subscription_id_sum_7d", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("web_event_page").orderBy("subscription_id_sum_7d", "id", "subscription_id").rowsBetween(-6, -1)
w10 = Window.partitionBy("device").orderBy("subscription_id", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.partitionBy("country").orderBy("subscription_id", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w12 = Window.partitionBy("device", "country").orderBy("monthly_fee_share")
w13 = Window.partitionBy("device").orderBy("subscription_id_sum_7d", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

matched = (
    df_sessions
    .alias("a")
    .join(df_subscriptions.alias("sub"), (F.col("a.customer_id") == F.col("sub.customer_id")) & (F.col("a.started_at") >= F.col("sub.start_date")) & (F.col("a.started_at") <= F.date_add(F.col("sub.end_date"), 10)), "left")
    .select(
        "a.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

with_windows = (
    matched
    .filter(F.col("country").isin(["JP", "DE", "US"]))
    .select(
        "id",
        "device",
        "country",
        "subscription_id",
        "monthly_fee",
        F.dense_rank().over(w).alias("subscription_id_dense_rank"),
        F.avg("monthly_fee").over(w2).alias("monthly_fee_avg_5"),
        F.sum("subscription_id").over(w_ordered).alias("subscription_id_sum_7d"),
    )
)

kept = (
    with_windows
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.session_id") == 6), "left_semi")
)

enriched = (
    kept
    .select(
        "*",
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w4)), 4).alias("monthly_fee_share"),
        F.avg("monthly_fee").over(w5).alias("monthly_fee_avg_2_prior"),
    )
)

with_last = (
    enriched
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.event_time").alias("web_event_event_time"),
        F.col("c.page").alias("web_event_page"),
    )
)

with_windows_sessions = (
    with_last
    .select(
        "*",
        F.sum("subscription_id_sum_7d").over(w7).alias("running_subscription_id_sum_7d"),
        F.sum("subscription_id_sum_7d").over(w8).alias("cumulative_subscription_id_sum_7d"),
        F.avg("subscription_id").over(w9).alias("subscription_id_avg_6_prior"),
        F.sum("subscription_id_dense_rank").over(w10).alias("cumulative_subscription_id_dense_rank"),
        F.sum("subscription_id_sum_7d").over(w11).alias("cumulative_subscription_id_sum_7d_2"),
    )
)

with_windows_sessions_2 = (
    with_windows_sessions
    .select(
        "id",
        "subscription_id",
        "monthly_fee_avg_5",
        "monthly_fee_share",
        "monthly_fee_avg_2_prior",
        "web_event_event_time",
        "running_subscription_id_sum_7d",
        "subscription_id_avg_6_prior",
        F.percent_rank().over(w12).alias("monthly_fee_share_percent_rank"),
        F.sum("subscription_id_avg_6_prior").over(w13).alias("running_subscription_id_avg_6_prior"),
    )
)

result = (
    with_windows_sessions_2
    .select(
        "id",
        "subscription_id",
        "monthly_fee_avg_5",
        "monthly_fee_share",
        "monthly_fee_avg_2_prior",
        "web_event_event_time",
        "subscription_id_avg_6_prior",
        "monthly_fee_share_percent_rank",
        "running_subscription_id_avg_6_prior",
    )
    .orderBy("web_event_event_time")
)
