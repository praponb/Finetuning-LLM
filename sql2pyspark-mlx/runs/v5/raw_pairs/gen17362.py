from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())

base = (
    df_sessions
    .filter((F.col("started_at") >= F.to_timestamp(F.lit("2023-05-17 13:00:00"))) & F.col("country").isNotNull())
    .select("*", F.length("device").alias("device_length"))
)

filtered = (
    base
    .filter(F.col("country").isNotNull())
    .select(
        "id",
        "device",
        "browser",
        "started_at",
        F.col("device_length").cast("string").alias("device_length_str"),
    )
)

not_flagged = (
    filtered
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.event_type") == "add_to_cart"), "left_anti")
)

latest_match = (
    not_flagged
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.device", "s.browser", "s.started_at", F.col("c.duration_sec"))
)

latest_match_sessions = (
    latest_match
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.device", "s.browser", F.col("c.event_type"))
)

eligible = (
    latest_match_sessions
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.event_type") == "purchase"), "left_anti")
)

result = eligible.select("id", "device", "browser", "event_type")
