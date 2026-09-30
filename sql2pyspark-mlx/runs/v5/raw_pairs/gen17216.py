from pyspark.sql import functions as F

remaining_keys = (
    df_sessions
    .select("browser", "customer_id")
    .exceptAll(
        df_sessions
        .filter(F.col("customer_id").isin([7, 18]))
        .select("browser", "customer_id")
    )
)

verified = (
    df_sessions
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.browser") == F.col("k.browser")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

enriched = (
    verified
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id").eqNullSafe(F.col("d.id")), "left")
    .select(
        "s.id",
        "s.customer_id",
        "s.device",
        "s.browser",
        F.col("d.credit_limit"),
        F.col("d.signup_date"),
    )
)

kept = (
    enriched
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") < F.col("s.signup_date")), "left_semi")
)

not_flagged = (
    kept
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.event_time") <= F.col("s.signup_date")), "left_semi")
)

result = (
    not_flagged
    .groupBy(
        F.when(F.col("credit_limit") >= 14100, "PREMIUM").otherwise("STANDARD").alias("credit_limit_tier"),
    )
    .agg(
        F.percentile_approx("credit_limit", [0.25, 0.5, 0.75]).alias("credit_limit_quartiles"),
        F.size(F.collect_set("device")).alias("num_devices"),
        F.sum("credit_limit").alias("total_credit_limit"),
    )
    .filter(F.col("total_credit_limit") > 0)
)
