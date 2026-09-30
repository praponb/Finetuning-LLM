from pyspark.sql import functions as F

base = (
    df_sessions
    .filter(F.col("customer_id").isNotNull() & (F.col("started_at") < "2023-07-01"))
    .select(
        "id",
        "customer_id",
        "browser",
        "country",
        F.hour("started_at").alias("started_at_hour"),
        F.coalesce("browser", F.lit("DIRECT")).alias("browser_filled"),
    )
)

kept_keys = (
    base
    .select("customer_id", "country")
    .intersect(
        df_sessions
        .filter(F.col("browser").isin(["Edge", "Firefox"]))
        .select("customer_id", "country")
    )
)

reconciled = (
    base
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

eligible = (
    reconciled
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.page") == "/checkout"), "left_anti")
)

kept = (
    eligible
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.session_id").isin([6, 23])), "left_anti")
)

base_sessions = (
    kept
    .filter(~F.col("country").isin(["UK", "JP"]))
    .select(
        "id",
        "customer_id",
        "started_at_hour",
        "browser_filled",
        F.round((F.col("started_at_hour") * 0.85), 2).alias("started_at_hour_scaled"),
    )
)

eligible_sessions = (
    base_sessions
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.page") != "/search"), "left_semi")
)

result = (
    eligible_sessions
    .select("id", "customer_id", "browser_filled", "started_at_hour_scaled")
    .orderBy(F.col("customer_id").asc_nulls_first())
)
