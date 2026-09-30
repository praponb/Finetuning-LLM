from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_country").orderBy("event_time", "id")
w2 = Window.partitionBy("session_country").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_web_events
    .alias("e")
    .join(df_sessions.alias("s"), F.col("e.session_id") == F.col("s.id"), "left")
    .select(
        "e.id",
        "e.session_id",
        "e.event_time",
        "e.duration_sec",
        F.col("s.country"),
        F.col("s.browser"),
    )
)

joined = (
    enriched
    .alias("src")
    .join(df_sessions.alias("b"), F.col("src.session_id") == F.col("b.id"), "inner")
    .select(
        "src.id",
        "src.session_id",
        "src.event_time",
        F.col("b.country").alias("session_country"),
        F.col("b.device"),
    )
)

flagged = (
    joined
    .withColumn("session_start_flag", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

islands = flagged.withColumn("session_id_2", F.sum("session_start_flag").over(w2))

gaps = (
    islands
    .withColumn("is_new_burst", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

islands_web_events = gaps.withColumn("session_no", F.sum("is_new_burst").over(w2))

daily = (
    islands_web_events
    .withColumn("session_start_flag_tier", F.when(F.col("session_start_flag") >= 1, "HIGH").otherwise("LOW"))
    .withColumn("is_new_burst_tier", F.when(F.col("is_new_burst") >= 1, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["session_start_flag_tier", "is_new_burst_tier"], ["session_start_flag_tier"]],
        "session_start_flag_tier",
        "is_new_burst_tier",
    )
    .agg(
        F.round(F.sum("is_new_burst") / F.nullif(F.count(F.when(F.col("device") == "mobile", 1)), F.lit(0)), 2).alias("is_new_burst_per_mobile"),
        F.sum(F.when(F.col("session_country") == "SG", F.col("is_new_burst")).otherwise(0)).alias("sg_is_new_burst"),
        F.grouping_id().alias("grouping_level"),
    )
    .filter(F.col("sg_is_new_burst") < 1000000)
)

result = (
    daily
    .select("session_start_flag_tier", "is_new_burst_per_mobile", "sg_is_new_burst")
    .orderBy("sg_is_new_burst")
)
