from pyspark.sql import functions as F

monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.avg("monthly_fee").alias("avg_subscription_monthly_fee"))

matched_keys = (
    df_sessions
    .select("id", "country")
    .exceptAll(
        df_sessions
        .filter(F.col("device") != "tablet")
        .select("id", "country")
    )
)

reconciled = (
    df_sessions
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

valid = (
    reconciled
    .filter(~F.col("browser").eqNullSafe(F.lit("Firefox")))
    .select(
        "id",
        "customer_id",
        "browser",
        F.trim("country").alias("clean_country"),
        F.col("started_at").cast("date").alias("started_at_day"),
    )
)

eligible = (
    valid
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.event_time") < F.col("s.started_at_day")), "left_anti")
)

not_flagged = (
    eligible
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.duration_sec").between(156, 391)), "left_anti")
)

with_dims = (
    not_flagged
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .select(
        "s.id",
        "s.customer_id",
        "s.browser",
        "s.started_at_day",
        F.col("c.segment"),
        F.col("c.city"),
    )
)

with_dims_sessions = (
    with_dims
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id") == F.col("p.id"), "inner")
    .filter(F.month(F.col("p.signup_date")).isin([3, 4, 11]))
    .select("e.*", F.col("p.age"), F.col("p.country"), F.col("p.signup_date"))
)

remaining_keys = (
    with_dims_sessions
    .select("id", "country")
    .exceptAll(
        df_sessions
        .filter(F.col("started_at") >= "2023-03-20")
        .select("id", "country")
    )
)

reconciled_sessions = (
    with_dims_sessions
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

result = (
    reconciled_sessions
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "browser",
        "started_at_day",
        "segment",
        "city",
        "age",
        "country",
        "signup_date",
        F.col("avg_subscription_monthly_fee"),
    )
    .orderBy(F.col("age").asc_nulls_first())
)
