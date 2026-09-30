from pyspark.sql import functions as F

remaining_keys = (
    df_sessions
    .select("customer_id")
    .intersectAll(
        df_sessions
        .filter(F.to_date("started_at") >= "2025-06-01")
        .select("customer_id")
    )
)

kept_rows = (
    df_sessions
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

kept = (
    kept_rows
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_type").isin(["search", "click", "page_view"])), "left_semi")
)

enriched = (
    kept
    .alias("src")
    .join(df_customers.alias("b"), F.col("src.customer_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.segment"), F.col("b.name"), F.col("b.email"))
)

with_dims = (
    enriched
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.email").alias("customer_email"))
)

eligible = (
    with_dims
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") < F.col("s.started_at")), "left_anti")
)

result = (
    eligible
    .groupBy(F.date_trunc("MONTH", "started_at").alias("activity_month"))
    .agg(
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("duration_min"),
        F.count(F.when(F.col("segment") == "Corporate", 1)).alias("corporate_count"),
        F.min("started_at").alias("started_at"),
        F.min("started_at").alias("first_seen"),
        F.count_if(F.col("segment") == "SMB").alias("smb_events"),
        F.count("*").alias("cnt"),
    )
)
