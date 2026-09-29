from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id")
w2 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())
w4 = Window.partitionBy("country", "browser").orderBy(F.col("duration_sec").desc(), "id")

flagged = (
    df_sessions
    .withColumn("session_start_flag", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("session_start_flag").over(w2))

with_last = (
    sessionized
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") <= F.col("s.started_at")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.event_time").alias("web_event_event_time"),
        F.col("c.duration_sec"),
    )
)

latest = (
    with_last
    .withColumn("latest_rank", F.row_number().over(w4))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "started_at",
        "session_start_flag",
        "session_id",
        "web_event_event_time",
        "duration_sec",
    )
)

with_last_sessions = (
    latest
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") <= F.col("s.started_at")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.session_start_flag",
        "s.web_event_event_time",
        F.col("c.event_type").alias("web_event_event_type"),
        F.col("c.page").alias("web_event_page"),
    )
)

result = (
    with_last_sessions
    .withColumn("year_start", F.date_trunc("YEAR", "web_event_event_time"))
    .withColumn("session_start_flag_tier", F.when(F.col("session_start_flag") >= 1, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["web_event_event_type", "year_start", "session_start_flag_tier"], ["web_event_event_type", "year_start"], ["web_event_event_type"]],
        "web_event_event_type",
        "year_start",
        "session_start_flag_tier",
    )
    .agg(
        F.count("*").alias("cnt"),
        F.sort_array(F.collect_set("web_event_event_type")).alias("web_event_event_type_list"),
        ((F.unix_timestamp(F.max("web_event_event_time")) - F.unix_timestamp(F.min("web_event_event_time"))) / 60).alias("duration_min"),
        F.count("*").alias("event_count"),
        F.max("web_event_event_time").alias("last_web_event_event_time"),
        F.count(F.when(F.col("web_event_page") == "/cart", 1)).alias("cart_count"),
    )
    .filter(F.col("cart_count") < 1000)
    .orderBy("web_event_event_type")
)
