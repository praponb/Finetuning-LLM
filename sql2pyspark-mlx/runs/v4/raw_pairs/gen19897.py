from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id", "page").orderBy(F.col("duration_sec").desc(), "id")
w2 = Window.partitionBy("event_type").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_web_events
    .filter((F.col("event_time") >= F.lit("2023-12-28 17:00:00").cast("timestamp")) & F.col("page").isNotNull() & F.col("duration_sec").between(136, 430))
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "duration_sec",
        F.when(F.col("page").isin(["/checkout", "/product"]), "Tier 1").otherwise("Other").alias("page_group"),
    )
)

enriched = (
    cleaned
    .alias("a")
    .join(df_sessions.alias("p"), F.col("a.session_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.device"), F.col("p.country"))
)

with_windows = (
    enriched
    .withColumn("duration_sec_bucket", F.ntile(3).over(w))
    .withColumn("first_known_page", F.first("page", ignorenulls=True).over(w2))
)

kept_keys = (
    with_windows
    .select("id")
    .intersect(
        df_web_events
        .filter(F.col("duration_sec").isNull())
        .select("id")
    )
)

verified = (
    with_windows
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

summary = (
    verified
    .groupBy(
        F.when(F.col("duration_sec") >= 449, "LARGE").otherwise("SMALL").alias("duration_sec_tier"),
        F.when(F.col("duration_sec_bucket") >= 7, "LARGE").otherwise("SMALL").alias("duration_sec_bucket_tier"),
    )
    .agg(
        F.sum(F.when(F.col("device") == "mobile", F.col("duration_sec")).otherwise(0)).alias("mobile_duration_sec"),
        F.avg("duration_sec_bucket").alias("avg_duration_sec_bucket"),
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        F.count("*").alias("row_count"),
        F.size(F.collect_set("device")).alias("num_devices"),
        F.percentile_approx("duration_sec_bucket", [0.25, 0.5, 0.75]).alias("duration_sec_bucket_quartiles"),
    )
    .filter(F.col("num_devices") > 1)
)

result = (
    summary
    .select(
        "duration_sec_tier",
        "duration_sec_bucket_tier",
        "avg_duration_sec_bucket",
        "avg_duration_sec",
        "row_count",
    )
    .orderBy(F.col("avg_duration_sec").desc_nulls_last(), "avg_duration_sec_bucket")
)
