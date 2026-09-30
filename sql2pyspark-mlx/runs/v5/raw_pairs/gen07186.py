from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device")
monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.min("monthly_fee").alias("min_subscription_monthly_fee"))

valid = (
    df_sessions
    .filter(F.col("country").isNotNull() & (F.col("started_at") >= "2023-10-01"))
    .select(
        "id",
        "customer_id",
        "device",
        "started_at",
        "country",
        F.dayofmonth("started_at").alias("started_at_day"),
    )
)

kept_keys = (
    valid
    .select("id", "country")
    .intersectAll(
        df_sessions
        .filter(F.col("country").isin(["US", "UK"]))
        .select("id", "country")
    )
)

verified = (
    valid
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

kept = (
    verified
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.hour(F.col("x.event_time")).between(10, 17)), "left_semi")
)

kept_keys_2 = (
    kept
    .select("id", "country")
    .exceptAll(
        df_sessions
        .filter(F.col("device") == "mobile")
        .select("id", "country")
    )
)

kept_rows = (
    kept
    .alias("s")
    .join(kept_keys_2.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

not_flagged = (
    kept_rows
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") >= F.col("s.started_at")), "left_anti")
)

with_dims = (
    not_flagged
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.email"))
)

metrics = (
    with_dims
    .filter(F.col("customer_id").isin([16, 28, 30]))
    .select("*", F.max("started_at_day").over(w).alias("device_max_started_at_day"))
)

stats = (
    metrics
    .withColumn("week_start", F.date_trunc("week", "started_at"))
    .groupingSets(
        [["week_start", "customer_id"], ["week_start"], []],
        "week_start",
        "customer_id",
    )
    .agg(
        F.percentile_approx("started_at_day", 0.9).alias("started_at_day_p90"),
        F.count(F.when(F.col("country") == "JP", 1)).alias("jp_rows"),
    )
)

result = (
    stats
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select("customer_id", "started_at_day_p90", F.col("min_subscription_monthly_fee"))
)
