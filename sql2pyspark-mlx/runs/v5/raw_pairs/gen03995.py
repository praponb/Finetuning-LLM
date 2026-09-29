from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "device").orderBy(F.col("duration_sec").asc_nulls_last(), "id")
w2 = Window.partitionBy("page", "device").orderBy(F.col("duration_sec").desc(), "id")

with_dims = (
    df_web_events
    .alias("a")
    .join(df_sessions.alias("p"), F.col("a.session_id") == F.col("p.id"), "left")
    .filter(~F.col("p.country").isin(["SG", "DE"]))
    .select(
        "a.id",
        "a.session_id",
        "a.event_type",
        "a.page",
        "a.duration_sec",
        F.col("p.country"),
        F.col("p.browser"),
        F.col("p.device"),
    )
)

latest = (
    with_dims
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

deduped = (
    latest
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
)

cleaned = (
    deduped
    .filter(F.col("country").isNotNull() & F.col("event_type").isin(["page_view", "purchase"]))
    .withColumn("device_group", F.when(F.col("device").isin(["desktop", "tablet"]), "Core").otherwise("Other"))
    .withColumn("resolved_page", F.coalesce("page", F.lit("NONE")))
)

joined = (
    cleaned
    .alias("src")
    .join(df_sessions.alias("p"), F.col("src.session_id") == F.col("p.id"), "left")
    .select(
        "src.id",
        "src.session_id",
        "src.event_type",
        "src.page",
        "src.browser",
        "src.device",
        "src.resolved_page",
        F.col("p.device").alias("session_device"),
        F.col("p.country").alias("session_country"),
    )
)

result = (
    joined
    .select("id", "session_id", "device", "resolved_page", "session_device")
    .orderBy(F.col("session_id").desc(), "session_device")
)
