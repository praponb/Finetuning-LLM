from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy(F.col("days_since_started_at").asc_nulls_first())
w2 = Window.partitionBy("segment")
w3 = Window.orderBy(F.col("credit_limit").desc_nulls_first())
w4 = Window.partitionBy("segment").orderBy(F.col("credit_limit_share").desc_nulls_first())
w5 = Window.partitionBy("customer_id", "segment").orderBy(F.col("credit_limit_share").desc())
w6 = Window.orderBy(F.col("days_since_started_at_rank").desc_nulls_first())
w_grp = Window.partitionBy("segment").orderBy(F.col("credit_limit_dense_rank_share").desc())
w8 = Window.partitionBy("segment").orderBy(F.col("credit_limit_share_percent_rank").desc())
w_recent = Window.partitionBy("segment").orderBy(F.col("credit_limit_share_rank").asc_nulls_first())

base = (
    df_sessions
    .filter(F.col("started_at") >= F.lit("2023-09-28 09:00:00").cast("timestamp"))
    .withColumn("device_group", F.when(F.col("device").isin(["tablet", "mobile"]), "Group A").otherwise("Other"))
    .withColumn("days_since_started_at", F.datediff(F.current_date(), "started_at"))
)

kept_keys = (
    base
    .select("id", "browser")
    .intersect(
        df_sessions
        .filter(F.hour("started_at").between(11, 15))
        .select("id", "browser")
    )
)

kept_rows = (
    base
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.browser") == F.col("k.browser")), "inner")
    .select("s.*")
)

not_flagged = (
    kept_rows
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") <= F.col("s.started_at")), "left_semi")
)

joined = (
    not_flagged
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "inner")
    .select(
        "a.id",
        "a.customer_id",
        "a.device",
        "a.device_group",
        "a.days_since_started_at",
        F.col("d.segment"),
        F.col("d.credit_limit"),
        F.col("d.signup_date"),
    )
)

enriched = (
    joined
    .select(
        "id",
        "customer_id",
        "device_group",
        "days_since_started_at",
        "segment",
        F.rank().over(w).alias("days_since_started_at_rank"),
        F.round((F.col("credit_limit") / F.sum("credit_limit").over(w2)), 4).alias("credit_limit_share"),
        F.dense_rank().over(w3).alias("credit_limit_dense_rank"),
    )
)

metrics = (
    enriched
    .select(
        "id",
        "device_group",
        "days_since_started_at",
        "segment",
        "days_since_started_at_rank",
        F.percent_rank().over(w4).alias("credit_limit_share_percent_rank"),
        F.round((F.col("credit_limit_dense_rank") / F.sum("credit_limit_dense_rank").over(w2)), 4).alias("credit_limit_dense_rank_share"),
        F.rank().over(w5).alias("credit_limit_share_rank"),
        F.cume_dist().over(w6).alias("days_since_started_at_rank_cume_dist"),
    )
)

metrics_sessions = (
    metrics
    .select(
        "id",
        "device_group",
        "days_since_started_at",
        "days_since_started_at_rank",
        F.dense_rank().over(w_grp).alias("credit_limit_dense_rank_share_dense_rank"),
        F.rank().over(w8).alias("credit_limit_share_percent_rank_rank"),
        F.stddev_samp("days_since_started_at_rank").over(w2).alias("segment_stddev_samp_days_since_started_at_rank"),
        F.percent_rank().over(w_recent).alias("credit_limit_share_rank_percent_rank"),
    )
)

result = (
    metrics_sessions
    .cube(
        F.when(F.col("credit_limit_share_rank_percent_rank") >= 0.89, "HIGH").otherwise("LOW").alias("credit_limit_share_rank_percent_rank_tier"),
    )
    .agg(
        F.percentile_approx("credit_limit_share_percent_rank_rank", 0.95).alias("credit_limit_share_percent_rank_rank_p95"),
        F.countDistinct("id").alias("distinct_ids"),
        F.min("days_since_started_at").alias("min_days_since_started_at"),
    )
    .orderBy(F.col("distinct_ids").desc_nulls_first())
)
