from pyspark.sql import functions as F

balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.avg("balance").alias("avg_account_balance"))

matched_keys = (
    df_sessions
    .select("browser")
    .subtract(
        df_sessions
        .filter(F.hour("started_at").between(7, 15))
        .select("browser")
    )
)

reconciled = (
    df_sessions
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.browser") == F.col("k.browser")), "inner")
    .select("s.*")
)

matched_keys_2 = (
    reconciled
    .select("id")
    .exceptAll(
        df_sessions
        .filter(F.col("device") == "tablet")
        .select("id")
    )
)

kept_rows = (
    reconciled
    .alias("s")
    .join(matched_keys_2.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

not_flagged = (
    kept_rows
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") <= F.col("s.started_at")), "left_anti")
)

not_flagged_sessions = (
    not_flagged
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") <= F.col("s.started_at")), "left_anti")
)

result = (
    not_flagged_sessions
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        F.col("avg_account_balance"),
    )
)
