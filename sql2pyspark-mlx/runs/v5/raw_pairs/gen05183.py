from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page", "session_id").orderBy(F.col("duration_sec").desc(), "id")
duration_sec_by_session_id = df_web_events.groupBy("session_id").agg(F.max("duration_sec").alias("max_web_event_duration_sec"))

base = (
    df_web_events
    .filter(F.col("page") != "/product")
    .select(
        "*",
        F.substring("event_type", 1, 4).alias("event_type_prefix"),
        F.coalesce("page", F.lit("DIRECT")).alias("page_clean"),
    )
)

ranked = (
    base
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

kept_keys = (
    ranked
    .select("event_type")
    .subtract(
        df_web_events
        .filter(F.col("event_type") != "page_view")
        .select("event_type")
    )
)

reconciled = (
    ranked
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.event_type") == F.col("k.event_type")), "inner")
    .select("s.*")
)

enriched = (
    reconciled
    .alias("s")
    .join(df_sessions.alias("d"), F.col("s.session_id") == F.col("d.id"), "left")
    .select(
        "s.id",
        "s.session_id",
        "s.event_type",
        "s.page",
        "s.event_time",
        "s.event_type_prefix",
        F.col("d.device"),
        F.col("d.country"),
        F.col("d.browser"),
    )
)

cleaned = (
    enriched
    .filter((F.col("event_time") >= F.to_timestamp(F.lit("2023-08-30 03:00:00"))) & F.col("browser").isNotNull())
    .withColumn("event_type_group", F.when(F.col("event_type").isin(["purchase", "click"]), "Core").otherwise("Other"))
)

result = (
    cleaned
    .join(duration_sec_by_session_id, on="session_id", how="left")
    .select(
        "session_id",
        "event_type",
        "page",
        "event_time",
        "event_type_prefix",
        "device",
        "country",
        "event_type_group",
        F.when(F.col("event_type").rlike("^(search|purchase)$"), "GROUP_A").when(F.col("event_type").rlike("^(page_view|click)$"), "PRIORITY").otherwise("CORE").alias("event_type_zone"),
        F.col("max_web_event_duration_sec"),
    )
    .orderBy(F.col("device").desc(), F.col("page").desc())
)
