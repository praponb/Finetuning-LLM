from pyspark.sql import functions as F

duration_sec_by_session_id = df_web_events.groupBy("session_id").agg(F.max("duration_sec").alias("max_web_event_duration_sec"))

enriched = (
    df_web_events
    .alias("s")
    .join(df_sessions.alias("d"), F.col("s.session_id") == F.col("d.id"), "inner")
    .filter(~F.col("d.browser").isin(["Safari", "Chrome"]))
    .select("s.*", F.col("d.device"), F.col("d.country"))
)

result = (
    enriched
    .join(duration_sec_by_session_id, on="session_id", how="left")
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "duration_sec",
        "country",
        F.when(F.col("country").rlike("^(DE|SG)$"), "CORE").when(F.col("country").rlike("^(TH|US)$"), "GROUP_B").otherwise("GROUP_A").alias("country_zone"),
        F.col("max_web_event_duration_sec"),
    )
    .orderBy(
        F.col("max_web_event_duration_sec").desc_nulls_last(),
        F.col("country_zone").desc(),
    )
)
