from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("days_since_event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("event_type").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("session_id", "event_type").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("session_id").orderBy("days_since_event_time", "id")

filtered = (
    df_web_events
    .filter(F.col("event_time") >= F.lit("2024-05-13").cast("date"))
    .select(
        "id",
        "session_id",
        "event_type",
        "duration_sec",
        F.when(F.col("page").isin(["/cart", "/search"]), "Priority").otherwise("Other").alias("page_group"),
        F.datediff(F.current_date(), "event_time").alias("days_since_event_time"),
    )
)

with_windows = (
    filtered
    .withColumn("running_days_since_event_time", F.sum("days_since_event_time").over(w))
    .withColumn("first_known_duration_sec", F.first("duration_sec", ignorenulls=True).over(w2))
    .withColumn("running_duration_sec", F.sum("duration_sec").over(w3))
    .withColumn("days_since_event_time_bucket", F.ntile(10).over(w4))
)

valid = (
    with_windows
    .filter(F.col("days_since_event_time") <= 42)
    .withColumn("duration_sec_per_first_known_duration_sec", (F.col("duration_sec") / F.nullif("first_known_duration_sec", F.lit(0))))
)

enriched = (
    valid
    .alias("src")
    .join(df_sessions.alias("b"), F.col("src.session_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.country"))
)

cleaned = (
    enriched
    .filter(F.col("running_duration_sec") <= 1308)
    .withColumn("event_type_upper", F.upper("event_type"))
    .withColumn("country_filled", F.coalesce("country", F.lit("DIRECT")))
)

enriched_web_events = (
    cleaned
    .alias("a")
    .join(df_sessions.alias("p"), F.col("a.session_id") == F.col("p.id"), "inner")
    .select("a.*", F.col("p.device"))
)

filtered_web_events = (
    enriched_web_events
    .filter(F.col("running_duration_sec").between(1186, 1252))
    .select(
        "*",
        F.substring("event_type", 1, 3).alias("event_type_prefix"),
        F.initcap(F.trim("device")).alias("device_title"),
        F.coalesce("device", F.lit("UNKNOWN")).alias("device_clean"),
    )
)

daily = (
    filtered_web_events
    .rollup("device")
    .agg(
        F.count(F.when(F.col("event_type") == "purchase", 1)).alias("purchase_rows"),
        F.count(F.when(F.col("country") == "US", 1)).alias("us_count"),
    )
)

result = (
    daily
    .select("us_count")
    .orderBy(F.col("us_count").desc())
)
