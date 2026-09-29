from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("event_time_date", "id").rowsBetween(-3, -1)
w2 = Window.orderBy("event_time", "id")

cleaned = (
    df_web_events
    .filter(F.col("page").isNotNull())
    .select(
        "*",
        F.concat_ws("-", "event_type", "page").alias("event_type_page_key"),
        F.to_date("event_time").alias("event_time_date"),
        F.coalesce("page", F.lit("OTHER")).alias("page_filled"),
    )
)

with_dims = (
    cleaned
    .alias("e")
    .join(df_sessions.alias("d"), F.col("e.session_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.device"), F.col("d.browser"), F.col("d.country"))
)

with_windows = (
    with_dims
    .select(
        "*",
        F.avg("duration_sec").over(w).alias("duration_sec_avg_3_prior"),
        F.row_number().over(w2).alias("row_seq"),
    )
)

kept_keys = (
    with_windows
    .select("page")
    .subtract(
        df_web_events
        .filter(F.col("duration_sec") == 406)
        .select("page")
    )
)

reconciled = (
    with_windows
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.page") == F.col("k.page")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .rollup(
        F.when(F.col("duration_sec_avg_3_prior") >= 452, "LARGE").otherwise("SMALL").alias("duration_sec_avg_3_prior_tier"),
        F.when(F.col("duration_sec") >= 395, "PREMIUM").otherwise("STANDARD").alias("duration_sec_tier"),
        F.date_trunc("quarter", "event_time").alias("quarter_start"),
    )
    .agg(
        F.min("event_time").alias("first_event_time"),
        F.count_if(F.col("event_type") == "search").alias("search_events"),
        F.size(F.collect_set("country")).alias("num_countries"),
    )
    .filter(F.col("num_countries") < 1)
)
