from pyspark.sql import functions as F

filtered = (
    df_sessions
    .filter((F.col("started_at") > F.lit("2023-06-07 22:00:00").cast("timestamp")) & (~F.col("browser").eqNullSafe(F.lit("Firefox"))))
    .select(
        "*",
        F.upper("browser").alias("browser_upper"),
        F.concat("browser", F.lit(" "), "device").alias("browser_device"),
    )
)

joined = (
    filtered
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "left")
    .select(
        "src.id",
        "src.customer_id",
        "src.device",
        "src.browser",
        "src.started_at",
        "src.browser_device",
        F.col("p.email"),
        F.col("p.credit_limit"),
        F.col("p.country").alias("customer_country"),
    )
)

matched_keys = (
    joined
    .select("customer_id")
    .subtract(
        df_sessions
        .filter(F.col("customer_id").isin([10, 18]))
        .select("customer_id")
    )
)

verified = (
    joined
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

enriched = (
    verified
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "left")
    .select(
        "src.*",
        F.col("c.credit_limit").alias("customer_credit_limit"),
        F.col("c.signup_date"),
    )
)

not_flagged = (
    enriched
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.duration_sec") < 177), "left_semi")
)

joined_sessions = (
    not_flagged
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id") == F.col("b.id"), "left")
    .select(
        "e.*",
        F.col("b.signup_date").alias("customer_signup_date"),
        F.col("b.country"),
    )
)

with_dims = (
    joined_sessions
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id") == F.col("b.id"), "left")
    .select(
        "e.customer_id",
        "e.browser",
        "e.browser_device",
        "e.email",
        "e.signup_date",
        "e.customer_signup_date",
        "e.country",
        F.col("b.country").alias("customer_country_2"),
        F.col("b.segment"),
    )
)

result = (
    with_dims
    .cube("segment")
    .agg(
        F.size(F.collect_set("country")).alias("num_countries"),
        F.countDistinct("segment").alias("distinct_segments"),
        F.sort_array(F.collect_set("customer_country_2")).alias("customer_country_2_list"),
        F.count(F.when(F.col("customer_country_2") == "JP", 1)).alias("jp_count"),
    )
)
