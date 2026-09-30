from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("session_id", "page").orderBy(F.col("duration_sec_rounded").desc_nulls_last(), "id")
w4 = Window.partitionBy("event_type", "session_id").orderBy(F.col("session_no").desc())
w5 = Window.partitionBy("session_id")

valid = (
    df_web_events
    .filter((F.col("event_time") > F.to_date(F.lit("2023-03-14"))) & (F.col("event_time") < "2025-09-01"))
    .select(
        "*",
        F.round("duration_sec", 2).alias("duration_sec_rounded"),
        F.coalesce("page", F.lit("N/A")).alias("page_clean"),
    )
)

flagged = (
    valid
    .withColumn("is_new_burst", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

islands = flagged.withColumn("session_no", F.sum("is_new_burst").over(w2))

ranked = (
    islands
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
)

kept_keys = (
    ranked
    .select("event_type", "page")
    .subtract(
        df_web_events
        .filter(F.col("session_id").isin([12, 29]))
        .select("event_type", "page")
    )
)

kept_rows = (
    ranked
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.event_type") == F.col("k.event_type")) & (F.col("s.page") == F.col("k.page")), "inner")
    .select("s.*")
)

metrics = (
    kept_rows
    .withColumn("session_no_dense_rank", F.dense_rank().over(w4))
    .withColumn("session_id_count_session_no", F.count("session_no").over(w5))
)

result = (
    metrics
    .alias("e")
    .join(df_sessions.alias("d"), F.col("e.session_id") == F.col("d.id"), "inner")
    .groupingSets(
        [["device", "page"], ["device"], []],
        F.col("d.device").alias("device"),
        F.col("e.page").alias("page"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.max(F.col("e.session_no")).alias("max_session_no"),
        F.min(F.col("e.duration_sec_rounded")).alias("min_duration_sec_rounded"),
        F.grouping_id().alias("level_id"),
    )
    .orderBy(F.col("max_session_no").desc_nulls_last())
)
