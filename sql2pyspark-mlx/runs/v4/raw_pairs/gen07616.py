from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_device", "browser").orderBy(F.col("event_time").desc_nulls_last())

enriched = (
    df_web_events
    .alias("s")
    .join(df_sessions.alias("p"), F.col("s.session_id") == F.col("p.id"), "inner")
    .select("s.*", F.col("p.country"), F.col("p.device"), F.col("p.browser"))
)

joined = (
    enriched
    .alias("src")
    .join(df_sessions.alias("p"), F.col("src.session_id") == F.col("p.id"), "inner")
    .select(
        "src.*",
        F.col("p.device").alias("session_device"),
        F.col("p.country").alias("session_country"),
        F.col("p.browser").alias("session_browser"),
    )
)

latest = (
    joined
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
)

result = (
    latest
    .groupBy(
        "event_type",
        F.when(F.col("duration_sec") >= 495, "PREMIUM").otherwise("STANDARD").alias("duration_sec_tier"),
    )
    .agg(
        F.min("duration_sec").alias("min_duration_sec"),
        ((F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))) / 60).alias("duration_min"),
    )
    .filter(F.col("min_duration_sec") > 0)
    .orderBy(F.col("event_type").desc())
)
