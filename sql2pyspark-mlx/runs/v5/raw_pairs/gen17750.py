from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("device").orderBy(F.col("web_event_event_time").desc(), "id")

latest_match = (
    df_sessions
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.device",
        "s.browser",
        F.col("c.event_time").alias("web_event_event_time"),
    )
)

kept = (
    latest_match
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.event_time") >= F.col("s.web_event_event_time")), "left_semi")
)

kept_sessions = (
    kept
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.event_time") < F.col("s.web_event_event_time")), "left_anti")
)

enriched = kept_sessions.select("*", F.row_number().over(w2).alias("row_seq"))

result = (
    enriched
    .select(
        "id",
        "device",
        "browser",
        "row_seq",
        F.when(F.col("device").rlike("^(mobile|tablet)$"), "CORE").otherwise("PRIORITY").alias("device_zone"),
    )
    .orderBy(F.col("row_seq").desc_nulls_first())
)
