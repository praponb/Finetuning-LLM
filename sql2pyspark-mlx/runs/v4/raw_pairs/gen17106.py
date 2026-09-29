from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.orderBy("event_time", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("device").orderBy("duration_sec", "id")

cleaned = (
    df_web_events
    .filter(F.col("session_id").isin([2, 12]))
    .select(
        "*",
        F.concat_ws("-", "page", "event_type").alias("page_event_type_key"),
        F.round("duration_sec", 1).alias("duration_sec_rounded"),
        F.coalesce("page", F.lit("DIRECT")).alias("page_clean"),
    )
)

enriched = (
    cleaned
    .alias("e")
    .join(df_sessions.alias("d"), F.col("e.session_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.device"), F.col("d.country"), F.col("d.browser"))
)

scored = (
    enriched
    .withColumn("duration_sec_rounded_avg_2", F.avg("duration_sec_rounded").over(w_ordered))
    .withColumn("duration_sec_bucket", F.ntile(5).over(w2))
)

cleaned_web_events = (
    scored
    .filter((F.col("event_time") >= F.lit("2023-10-05").cast("date")) & F.col("device").isNotNull())
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        "page_event_type_key",
        "duration_sec_rounded",
        "device",
        "browser",
        "duration_sec_rounded_avg_2",
        (F.col("duration_sec_bucket") + F.col("duration_sec_rounded_avg_2")).alias("duration_sec_bucket_plus_duration_sec_rounded_avg_2"),
    )
)

result = (
    cleaned_web_events
    .groupBy(
        F.date_trunc("week", "event_time").alias("week_start"),
        F.when(F.col("duration_sec_bucket_plus_duration_sec_rounded_avg_2") >= 66, "PREMIUM").otherwise("STANDARD").alias("duration_sec_bucket_plus_duration_sec_rounded_avg_2_tier"),
    )
    .agg(
        F.count(F.when(F.col("event_type") == "purchase", 1)).alias("purchase_rows"),
        F.sort_array(F.collect_set("browser")).alias("browser_list"),
    )
    .orderBy("duration_sec_bucket_plus_duration_sec_rounded_avg_2_tier")
)
