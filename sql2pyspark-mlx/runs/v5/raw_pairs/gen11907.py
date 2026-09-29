from pyspark.sql import functions as F

with_dims = (
    df_sessions
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .select("s.*", F.col("c.segment"), F.col("c.credit_limit"))
)

remaining_keys = (
    with_dims
    .select("device")
    .intersectAll(
        df_sessions
        .filter(F.col("customer_id").isNull())
        .select("device")
    )
)

verified = (
    with_dims
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.device") == F.col("k.device")), "inner")
    .select("s.*")
)

eligible = (
    verified
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.event_time") >= F.col("s.started_at")), "left_anti")
)

kept = (
    eligible
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.session_id").isin([18, 38])), "left_anti")
)

with_dims_sessions = (
    kept
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "left")
    .select(
        "src.id",
        "src.customer_id",
        "src.segment",
        "src.credit_limit",
        F.col("p.city"),
    )
)

eligible_sessions = (
    with_dims_sessions
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.event_type") == "purchase"), "left_anti")
)

totals = (
    eligible_sessions
    .withColumn("credit_limit_tier", F.when(F.col("credit_limit") >= 14800, "LARGE").otherwise("SMALL"))
    .cube("credit_limit_tier")
    .agg(
        F.count(F.when(F.col("city") == "London", 1)).alias("london_rows"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.max(F.when(F.col("segment") == "SMB", F.col("credit_limit"))).alias("smb_max_credit_limit"),
        F.grouping("credit_limit_tier").alias("grouping_level"),
    )
)

result = (
    totals
    .select("credit_limit_tier", "city_list", "smb_max_credit_limit", "grouping_level")
    .orderBy(F.col("smb_max_credit_limit").desc_nulls_last())
)
