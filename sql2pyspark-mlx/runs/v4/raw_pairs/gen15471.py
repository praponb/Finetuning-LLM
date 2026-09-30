from pyspark.sql import functions as F

remaining_keys = (
    df_sessions
    .select("country")
    .subtract(
        df_sessions
        .filter(F.col("country") == "DE")
        .select("country")
    )
)

kept_rows = (
    df_sessions
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

not_flagged = (
    kept_rows
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.event_time") < F.col("s.started_at")), "left_semi")
)

with_dims = (
    not_flagged
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id") == F.col("d.id"), "inner")
    .select(
        "src.*",
        F.col("d.city"),
        F.col("d.signup_date"),
        F.col("d.country").alias("customer_country"),
    )
)

enriched = (
    with_dims
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "inner")
    .select(
        "a.*",
        F.col("p.name"),
        F.col("p.signup_date").alias("customer_signup_date"),
        F.col("p.email"),
    )
)

eligible = (
    enriched
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.event_type").isin(["page_view", "purchase", "add_to_cart"])), "left_anti")
)

eligible_sessions = (
    eligible
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.event_time") >= F.col("s.signup_date")), "left_anti")
)

totals = (
    eligible_sessions
    .groupBy(F.date_trunc("WEEK", "customer_signup_date").alias("week_start"))
    .agg(
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("duration_min"),
        F.count_if(F.col("customer_country") == "SG").alias("sg_events"),
        F.countDistinct("city").alias("distinct_cities"),
    )
)

result = (
    totals
    .select("week_start", "duration_min", "sg_events", "distinct_cities")
    .orderBy(F.col("duration_min").desc_nulls_first())
)
