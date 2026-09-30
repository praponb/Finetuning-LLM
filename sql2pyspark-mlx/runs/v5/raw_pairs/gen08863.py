from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("device").orderBy("web_event_duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("web_event_duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_sessions
    .filter(F.col("browser").isNotNull())
    .select(
        "*",
        F.initcap(F.trim("browser")).alias("browser_title"),
        F.coalesce("browser", F.lit("OTHER")).alias("browser_filled"),
    )
)

latest_match = (
    cleaned
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.device",
        "s.country",
        F.col("c.duration_sec").alias("web_event_duration_sec"),
    )
)

with_windows = (
    latest_match
    .filter(F.col("device") == "desktop")
    .select(
        "id",
        "web_event_duration_sec",
        F.first("country", ignorenulls=True).over(w2).alias("first_known_country"),
        F.last("web_event_duration_sec", ignorenulls=True).over(w3).alias("last_known_web_event_duration_sec"),
    )
)

latest_match_sessions = (
    with_windows
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.event_time").alias("web_event_event_time"))
)

result = (
    latest_match_sessions
    .select("id", "web_event_duration_sec", "last_known_web_event_duration_sec")
)
