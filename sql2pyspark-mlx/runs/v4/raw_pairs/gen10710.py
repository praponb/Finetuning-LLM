from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("session_id").orderBy(F.col("event_time").desc_nulls_last(), "id")

with_gaps = (
    df_web_events
    .withColumn("is_new_session", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("island_id", F.sum("is_new_session").over(w2))

ranked = (
    sessionized
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
)

enriched = (
    ranked
    .alias("src")
    .join(df_sessions.alias("s"), F.col("src.session_id") == F.col("s.id"), "left")
    .select("src.*", F.col("s.browser"), F.col("s.country"), F.col("s.device"))
)

result = (
    enriched
    .select(
        "id",
        "event_type",
        "page",
        "event_time",
        "is_new_session",
        "island_id",
        "browser",
        "country",
        "device",
        F.when(F.col("browser").rlike("^(Firefox|Safari)$"), "EXTENDED").when(F.col("browser").rlike("^(Chrome|Edge)$"), "OTHER_ZONE").otherwise("PRIORITY").alias("browser_zone"),
    )
    .orderBy(F.col("is_new_session").desc(), "country")
)
