from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy(F.col("duration_sec").desc_nulls_first())
duration_sec_by_session_id = df_web_events.groupBy("session_id").agg(F.min("duration_sec").alias("min_web_event_duration_sec"))

valid = (
    df_web_events
    .filter(F.col("event_time") >= F.lit("2023-11-21 22:00:00").cast("timestamp"))
    .select(
        "*",
        F.when(F.col("event_type").isin(["add_to_cart", "click"]), "Group A").otherwise("Other").alias("event_type_group"),
        F.concat("page", F.lit(" "), "event_type").alias("page_event_type"),
    )
)

enriched = (
    valid
    .alias("s")
    .join(df_sessions.alias("p"), F.col("s.session_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.browser"), F.col("p.country"))
)

ranked = (
    enriched
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
)

result = (
    ranked
    .join(duration_sec_by_session_id, on="session_id", how="left")
    .select(
        "id",
        "session_id",
        "event_type",
        "event_time",
        "duration_sec",
        "event_type_group",
        "page_event_type",
        F.when(F.col("event_type").rlike("^(add_to_cart|purchase)$"), "GROUP_B").when(F.col("event_type").rlike("^(page_view|click)$"), "CORE").otherwise("EXTENDED").alias("event_type_zone"),
        F.col("min_web_event_duration_sec"),
    )
    .orderBy(F.col("session_id").desc())
)
