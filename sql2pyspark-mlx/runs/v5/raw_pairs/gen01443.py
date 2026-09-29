from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_web_events
    .filter((F.col("event_time") > F.to_date(F.lit("2023-05-11"))) & (F.col("page") == "/cart"))
    .select(
        "id",
        "session_id",
        "event_type",
        "event_time",
        "duration_sec",
        F.round((F.col("duration_sec") * 100), 2).alias("adjusted_duration_sec"),
    )
)

with_dims = (
    valid
    .alias("s")
    .join(df_sessions.alias("p"), F.col("s.session_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.country"), F.col("p.browser"))
)

gaps = (
    with_dims
    .withColumn("is_new_session", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessionized = (
    gaps
    .select(
        "id",
        "session_id",
        "event_type",
        "event_time",
        "duration_sec",
        "country",
        "browser",
        F.sum("is_new_session").over(w2).alias("session_id_2"),
    )
)

filtered = (
    sessionized
    .filter(F.col("browser").isNotNull())
    .select(
        "id",
        "session_id",
        "event_time",
        "duration_sec",
        "country",
        "session_id_2",
        F.length("browser").alias("browser_length"),
        F.hour("event_time").alias("event_time_hour"),
    )
)

result = (
    filtered
    .groupBy("session_id")
    .pivot("country", ["SG", "US"])
    .agg(F.max("session_id_2").alias("total"), F.count(F.lit(1)).alias("cnt"))
)
