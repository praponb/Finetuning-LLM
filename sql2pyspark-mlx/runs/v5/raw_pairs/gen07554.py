from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("event_time").desc_nulls_last(), "id")
w2 = Window.partitionBy("device").orderBy(F.col("duration_sec").desc(), "id")
w3 = Window.partitionBy("country", "device").orderBy(F.col("duration_sec").desc(), "id")

enriched = (
    df_web_events
    .alias("s")
    .join(df_sessions.alias("d"), F.col("s.session_id") == F.col("d.id"), "left")
    .select(
        "s.id",
        "s.session_id",
        "s.page",
        "s.event_time",
        "s.duration_sec",
        F.col("d.device"),
        F.col("d.country"),
    )
)

latest = (
    enriched
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "duration_sec", "device", "country")
)

ranked = (
    latest
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

latest_web_events = (
    ranked
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
)

base = (
    latest_web_events
    .filter(F.col("device").isNotNull() & (F.col("device").eqNullSafe(F.lit("tablet"))))
    .select(
        "id",
        "duration_sec",
        "device",
        F.coalesce("duration_sec", F.lit(0)).alias("duration_sec_filled"),
        F.coalesce("device", F.lit("DIRECT")).alias("device_clean"),
    )
)

totals = (
    base
    .groupBy(
        F.when(F.col("duration_sec_filled") >= 76, "LARGE").otherwise("SMALL").alias("duration_sec_filled_tier"),
    )
    .agg(
        F.sum(F.when(F.col("device") == "desktop", F.col("duration_sec_filled")).otherwise(0)).alias("desktop_duration_sec_filled"),
        F.count(F.when(F.col("device") == "tablet", 1)).alias("tablet_count"),
        F.count(F.when(F.col("device") == "mobile", 1)).alias("mobile_rows"),
    )
)

result = totals.select("duration_sec_filled_tier", "tablet_count", "mobile_rows")
