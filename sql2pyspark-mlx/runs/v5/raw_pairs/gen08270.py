from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())

latest_match = (
    df_sessions
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") <= F.col("s.started_at")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.browser",
        F.col("c.duration_sec").alias("web_event_duration_sec"),
        F.col("c.page").alias("web_event_page"),
    )
)

result = (
    latest_match
    .select(
        "id",
        "browser",
        "web_event_duration_sec",
        "web_event_page",
        F.when(F.col("browser").rlike("^(Chrome|Firefox)$"), "CORE").when(F.col("browser").rlike("^(Safari|Edge)$"), "OTHER_ZONE").otherwise("GROUP_A").alias("browser_zone"),
    )
)
