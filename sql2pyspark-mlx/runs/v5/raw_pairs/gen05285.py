from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy(F.col("duration_sec").desc(), "id")
duration_sec_by_session_id = df_web_events.groupBy("session_id").agg(F.min("duration_sec").alias("min_web_event_duration_sec"))

filtered = (
    df_web_events
    .filter((F.col("event_time") > F.to_date(F.lit("2023-06-05"))) & F.col("page").isNotNull())
    .withColumn("event_type_group", F.when(F.col("event_type").isin(["add_to_cart", "click"]), "Group A").otherwise("Other"))
    .withColumn("page_filled", F.coalesce("page", F.lit("UNKNOWN")))
)

deduped = (
    filtered
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

enriched = (
    deduped
    .alias("src")
    .join(df_sessions.alias("b"), F.col("src.session_id") == F.col("b.id"), "left")
    .select(
        "src.id",
        "src.session_id",
        "src.page",
        "src.event_type_group",
        F.col("b.device"),
        F.col("b.browser"),
        F.col("b.country"),
    )
)

base = (
    enriched
    .filter(F.col("device").isNotNull() & (~F.col("country").eqNullSafe(F.lit("UK"))))
    .select(
        "id",
        "session_id",
        "page",
        "event_type_group",
        "browser",
        "country",
        F.concat_ws("-", "page", "browser").alias("page_browser_key"),
        F.initcap(F.trim("country")).alias("country_title"),
    )
)

valid = (
    base
    .filter(F.col("browser").isNotNull())
    .withColumn("country_group", F.when(F.col("country").isin(["SG", "US"]), "Core").otherwise("Other"))
    .withColumn("country_filled", F.coalesce("country", F.lit("NONE")))
)

with_dims = (
    valid
    .alias("src")
    .join(df_sessions.alias("d"), F.col("src.session_id") == F.col("d.id"), "left")
    .select(
        "src.id",
        "src.session_id",
        "src.page",
        "src.page_browser_key",
        "src.country_title",
        "src.country_group",
        F.col("d.country").alias("session_country"),
        F.col("d.device"),
    )
)

base_web_events = (
    with_dims
    .filter(F.col("page").isin(["/product", "/search", "/checkout"]))
    .select(
        "id",
        "session_id",
        "page_browser_key",
        "country_title",
        "device",
        F.concat("page", F.lit(", "), "device").alias("page_device"),
    )
)

result = (
    base_web_events
    .join(duration_sec_by_session_id, on="session_id", how="left")
    .select(
        "id",
        "session_id",
        "country_title",
        "device",
        F.when(F.col("device").rlike("^(desktop|tablet)$"), "EXTENDED").when(F.col("device").rlike("^(mobile)$"), "PRIORITY").otherwise("CORE").alias("device_zone"),
        F.col("min_web_event_duration_sec"),
    )
    .orderBy(F.col("device").desc_nulls_last())
)
