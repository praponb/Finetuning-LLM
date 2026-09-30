from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("event_type", "page")
w_part = Window.orderBy(F.col("event_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w3 = Window.partitionBy("page", "event_type")
w_recent = Window.orderBy(F.col("duration_sec").desc(), "id")
w5 = Window.partitionBy("session_id").orderBy(F.col("duration_sec_sum_30d").desc(), "id")
w6 = Window.partitionBy("country").orderBy("duration_sec", "id")
w7 = Window.orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy(F.col("duration_sec").asc_nulls_last())
duration_sec_by_session_id = df_web_events.groupBy("session_id").agg(F.max("duration_sec").alias("max_web_event_duration_sec"))

base = (
    df_web_events
    .filter(F.col("event_time") >= F.lit("2023-10-15 11:00:00").cast("timestamp"))
    .select(
        "*",
        F.initcap(F.trim("event_type")).alias("event_type_title"),
        F.when(F.col("event_type").isin(["search", "add_to_cart"]), "Tier 1").otherwise("Other").alias("event_type_group"),
    )
)

enriched = (
    base
    .select(
        "*",
        F.stddev_samp("duration_sec").over(w_hist).alias("event_type_stddev_samp_duration_sec"),
        F.sum("duration_sec").over(w_part).alias("duration_sec_sum_30d"),
        F.stddev_samp("duration_sec").over(w3).alias("page_stddev_samp_duration_sec"),
        F.ntile(5).over(w_recent).alias("duration_sec_bucket"),
    )
)

deduped = (
    enriched
    .withColumn("row_num", F.row_number().over(w5))
    .filter(F.col("row_num") == 1)
)

with_dims = (
    deduped
    .alias("src")
    .join(df_sessions.alias("p"), F.col("src.session_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.country"))
)

valid = (
    with_dims
    .filter((F.col("event_time") >= F.lit("2023-12-11 12:00:00").cast("timestamp")) & F.col("page").isNotNull() & (F.col("page_stddev_samp_duration_sec") >= 400))
    .select(
        "id",
        "session_id",
        "event_type",
        "event_time",
        "duration_sec",
        "event_type_title",
        "event_type_group",
        "duration_sec_bucket",
        "country",
        F.when(F.col("page_stddev_samp_duration_sec") >= 369, "High").when(F.col("page_stddev_samp_duration_sec") >= 84, "Medium").otherwise("Low").alias("page_stddev_samp_duration_sec_tier"),
        F.date_format("event_time", "MMM yyyy").alias("event_time_label"),
        F.coalesce("page", F.lit("UNKNOWN")).alias("page_clean"),
    )
)

scored = (
    valid
    .select(
        "*",
        F.lead("duration_sec", 2, 0).over(w6).alias("next_duration_sec"),
        F.first("duration_sec", ignorenulls=True).over(w7).alias("first_known_duration_sec"),
        F.rank().over(w8).alias("duration_sec_rank"),
    )
)

enriched_web_events = (
    scored
    .alias("src")
    .join(df_sessions.alias("d"), F.col("src.session_id") == F.col("d.id"), "inner")
    .select("src.*", F.col("d.browser"))
)

grouped = (
    enriched_web_events
    .groupBy(
        F.when(F.col("duration_sec") >= 530, "PREMIUM").otherwise("STANDARD").alias("duration_sec_tier"),
        "session_id",
        F.when(F.col("duration_sec_bucket") >= 9, "PREMIUM").otherwise("STANDARD").alias("duration_sec_bucket_tier"),
    )
    .agg(
        F.max(F.when(F.col("country") == "SG", F.col("next_duration_sec"))).alias("sg_max_next_duration_sec"),
        F.size(F.collect_set("event_type")).alias("num_event_types"),
        F.avg(F.when(F.col("browser") == "Chrome", F.col("first_known_duration_sec"))).alias("chrome_avg_first_known_duration_sec"),
    )
)

result = (
    grouped
    .join(duration_sec_by_session_id, on="session_id", how="left")
    .select(
        "duration_sec_tier",
        "session_id",
        "duration_sec_bucket_tier",
        "sg_max_next_duration_sec",
        "num_event_types",
        "chrome_avg_first_known_duration_sec",
        F.col("max_web_event_duration_sec"),
    )
    .orderBy(
        F.col("max_web_event_duration_sec").desc_nulls_first(),
        F.col("duration_sec_bucket_tier").desc(),
    )
)
