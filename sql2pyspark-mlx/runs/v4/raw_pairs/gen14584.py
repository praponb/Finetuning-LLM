from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy(F.col("duration_sec").desc(), "id")
w_grp = Window.partitionBy("session_id", "page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("session_id").orderBy("duration_sec", "id").rowsBetween(-5, Window.currentRow)
w4 = Window.partitionBy("session_device").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)

enriched = (
    df_web_events
    .withColumn("duration_sec_bucket", F.ntile(3).over(w))
    .withColumn("running_duration_sec", F.sum("duration_sec").over(w_grp))
)

joined = (
    enriched
    .alias("a")
    .join(df_sessions.alias("b"), F.col("a.session_id") == F.col("b.id"), "left")
    .select("a.*", F.col("b.device"), F.col("b.browser"))
)

enriched_web_events = (
    joined
    .alias("s")
    .join(df_sessions.alias("p"), F.col("s.session_id") == F.col("p.id"), "left")
    .select(
        "s.*",
        F.col("p.country"),
        F.col("p.browser").alias("session_browser"),
        F.col("p.device").alias("session_device"),
    )
)

scored = (
    enriched_web_events
    .select(
        "*",
        F.avg("running_duration_sec").over(w_hist).alias("running_duration_sec_avg_5"),
        F.count("duration_sec").over(w4).alias("duration_sec_count_30d"),
    )
)

with_dims = (
    scored
    .alias("s")
    .join(df_sessions.alias("d"), F.col("s.session_id") == F.col("d.id"), "inner")
    .select(
        "s.*",
        F.col("d.country").alias("session_country"),
        F.col("d.device").alias("session_device_2"),
    )
)

with_dims_web_events = (
    with_dims
    .alias("s")
    .join(df_sessions.alias("p"), F.col("s.session_id") == F.col("p.id"), "inner")
    .select(
        "s.*",
        F.col("p.country").alias("session_country_2"),
        F.col("p.device").alias("session_device_3"),
    )
)

joined_web_events = (
    with_dims_web_events
    .alias("src")
    .join(df_sessions.alias("p"), F.col("src.session_id") == F.col("p.id"), "left")
    .select(
        "src.*",
        F.col("p.country").alias("session_country_3"),
        F.col("p.device").alias("session_device_4"),
        F.col("p.browser").alias("session_browser_2"),
    )
)

result = (
    joined_web_events
    .cube(
        F.when(F.col("running_duration_sec_avg_5") >= 1848, "LARGE").otherwise("SMALL").alias("running_duration_sec_avg_5_tier"),
        F.when(F.col("duration_sec") >= 488, "HIGH").otherwise("LOW").alias("duration_sec_tier"),
    )
    .agg(
        F.count(F.when(F.col("session_device_3") == "desktop", 1)).alias("desktop_count"),
        F.count_if(F.col("country") == "US").alias("us_events"),
        F.count(F.when(F.col("page") == "/home", 1)).alias("home_rows"),
        F.bit_and("duration_sec").alias("duration_sec_bit_and"),
        F.bit_and("duration_sec").alias("duration_sec_bit_and_2"),
        F.countDistinct("session_id").alias("distinct_session_ids"),
    )
)
