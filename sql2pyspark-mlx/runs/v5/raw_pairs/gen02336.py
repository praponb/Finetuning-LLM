from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(-6, Window.currentRow)

cleaned = (
    df_web_events
    .filter(F.col("event_time") > F.lit("2023-07-02").cast("date"))
    .withColumn("event_type_page", F.concat("event_type", F.lit(" - "), "page"))
)

scored = (
    cleaned
    .select(
        "id",
        "session_id",
        "event_type",
        "event_time",
        "event_type_page",
        F.avg("duration_sec").over(w).alias("running_avg_duration_sec"),
        F.count("duration_sec").over(w).alias("running_count_duration_sec"),
        F.min("duration_sec").over(w).alias("running_min_duration_sec"),
        F.max("duration_sec").over(w).alias("running_max_duration_sec"),
        F.avg("duration_sec").over(w2).alias("duration_sec_avg_6"),
    )
)

matched_keys = (
    scored
    .select("session_id")
    .subtract(
        df_web_events
        .filter(~F.col("event_type").isin(["purchase", "add_to_cart", "search"]))
        .select("session_id")
    )
)

reconciled = (
    scored
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.session_id") == F.col("k.session_id")), "inner")
    .select("s.*")
)

filtered = (
    reconciled
    .filter(F.col("running_avg_duration_sec") <= 250)
    .select(
        "*",
        F.when(F.col("duration_sec_avg_6") >= 406, "High").when(F.col("duration_sec_avg_6") >= 101, "Medium").otherwise("Low").alias("duration_sec_avg_6_level"),
    )
)

with_dims = (
    filtered
    .alias("s")
    .join(df_sessions.alias("d"), F.col("s.session_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.browser"), F.col("d.country"), F.col("d.device"))
)

result = (
    with_dims
    .select(
        "device",
        "running_max_duration_sec",
        F.col("running_min_duration_sec").alias("RUNNING_MIN_DURATION_SEC"),
        F.col("running_avg_duration_sec").alias("RUNNING_AVG_DURATION_SEC"),
    )
    .unpivot(
        ids=["device"],
        values=["running_max_duration_sec", "RUNNING_MIN_DURATION_SEC", "RUNNING_AVG_DURATION_SEC"],
        variableColumnName="metric",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
