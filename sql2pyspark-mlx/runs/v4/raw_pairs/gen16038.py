from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())

with_latest = (
    df_sessions
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.event_time").alias("web_event_event_time"), F.col("c.page"))
)

result = (
    with_latest
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "page",
        F.when(F.col("device").rlike("^(desktop|tablet)$"), "OTHER_ZONE").otherwise("GROUP_B").alias("device_zone"),
    )
    .orderBy(F.col("id").asc_nulls_first())
)
