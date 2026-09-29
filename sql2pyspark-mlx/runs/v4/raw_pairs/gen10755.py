from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())

latest_match = (
    df_sessions
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.event_type"), F.col("c.page").alias("web_event_page"))
)

result = (
    latest_match
    .select(
        "id",
        "device",
        "browser",
        "started_at",
        "event_type",
        "web_event_page",
        F.when(F.col("device").rlike("^(desktop|tablet)$"), "GROUP_B").otherwise("PRIORITY").alias("device_zone"),
    )
    .orderBy(F.col("event_type").desc_nulls_last(), "browser")
)
