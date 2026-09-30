from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("browser").orderBy(F.col("event_time").desc())
w3 = Window.partitionBy("web_event_page").orderBy("event_time", "id")
w4 = Window.partitionBy("web_event_page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_sessions
    .filter(F.col("browser").eqNullSafe(F.lit("Edge")))
    .select(
        "id",
        "device",
        "browser",
        "country",
        F.when(F.col("device").isin(["tablet", "mobile"]), "Group A").otherwise("Other").alias("device_group"),
        F.substring("device", 1, 4).alias("device_prefix"),
        F.coalesce("browser", F.lit("NONE")).alias("resolved_browser"),
    )
)

with_last = (
    valid
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.browser",
        "s.country",
        "s.device_prefix",
        "s.resolved_browser",
        F.col("c.page").alias("web_event_page"),
        F.col("c.event_time"),
    )
)

deduped = (
    with_last
    .withColumn("row_num", F.rank().over(w2))
    .filter(F.col("row_num") == 1)
)

gaps = (
    deduped
    .withColumn("session_start_flag", F.when(F.col("event_time") - F.lag("event_time").over(w3) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

islands = (
    gaps
    .select(
        "id",
        "device_prefix",
        "web_event_page",
        "event_time",
        "row_num",
        F.sum("session_start_flag").over(w4).alias("session_no"),
    )
)

result = (
    islands
    .select("id", "web_event_page", "event_time", "row_num", "session_no")
    .orderBy("id")
    .limit(20)
)
