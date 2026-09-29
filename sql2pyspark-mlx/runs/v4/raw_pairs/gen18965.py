from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")
w2 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("event_type").orderBy(F.col("event_type_length").desc_nulls_last(), "id")

cleaned = (
    df_web_events
    .filter(~F.col("page").isin(["/checkout", "/home", "/cart"]))
    .select(
        "id",
        "session_id",
        "event_type",
        "event_time",
        "duration_sec",
        F.date_format("event_time", "yyyy-MM").alias("event_time_label"),
        F.length("event_type").alias("event_type_length"),
    )
)

gaps = (
    cleaned
    .withColumn("new_group_flag", F.when(F.col("event_time") - F.lag("event_time").over(w) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

islands = gaps.withColumn("session_id_2", F.sum("new_group_flag").over(w2))

deduped = (
    islands
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
)

remaining_keys = (
    deduped
    .select("id", "event_type")
    .subtract(
        df_web_events
        .filter(F.col("event_type") != "click")
        .select("id", "event_type")
    )
)

reconciled = (
    deduped
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.event_type") == F.col("k.event_type")), "inner")
    .select("s.*")
)

with_dims = (
    reconciled
    .alias("a")
    .join(df_sessions.alias("b"), F.col("a.session_id") == F.col("b.id"), "left")
    .select("a.*", F.col("b.device"))
)

joined = (
    with_dims
    .alias("a")
    .join(df_sessions.alias("s"), F.col("a.session_id") == F.col("s.id"), "left")
    .select("a.*", F.col("s.country"))
)

joined_web_events = (
    joined
    .alias("a")
    .join(df_sessions.alias("s"), F.col("a.session_id") == F.col("s.id"), "left")
    .select("a.*", F.col("s.country").alias("session_country"), F.col("s.browser"))
)

wide = (
    joined_web_events
    .groupBy("session_id")
    .pivot("browser", ["Chrome", "Firefox", "Safari"])
    .agg(F.sum("session_id_2").alias("amount"))
)

result = wide.select("session_id", "Firefox", "Safari")
