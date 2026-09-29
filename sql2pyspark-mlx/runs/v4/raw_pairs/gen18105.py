from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("page", "session_id").orderBy(F.col("duration_sec_rounded").asc_nulls_first())
w_recent_2 = Window.partitionBy("session_id")

base = (
    df_web_events
    .filter(F.col("page") == "/search")
    .select(
        "id",
        "session_id",
        "page",
        "event_time",
        "duration_sec",
        F.round("duration_sec", 1).alias("duration_sec_rounded"),
    )
)

matched_keys = (
    base
    .select("id")
    .subtract(
        df_web_events
        .filter(F.col("page").isin(["/checkout", "/cart"]))
        .select("id")
    )
)

kept_rows = (
    base
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = (
    kept_rows
    .withColumn("duration_sec_rounded_percent_rank", F.percent_rank().over(w_recent))
    .withColumn("session_id_sum_duration_sec_rounded", F.sum("duration_sec_rounded").over(w_recent_2))
    .withColumn("session_id_avg_duration_sec", F.avg("duration_sec").over(w_recent_2))
)

result = (
    enriched
    .withColumn("session_id_sum_duration_sec_rounded_tier", F.when(F.col("session_id_sum_duration_sec_rounded") >= 409, "PREMIUM").otherwise("STANDARD"))
    .withColumn("duration_sec_rounded_percent_rank_tier", F.when(F.col("duration_sec_rounded_percent_rank") >= 0.81, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["session_id_sum_duration_sec_rounded_tier", "duration_sec_rounded_percent_rank_tier"], ["session_id_sum_duration_sec_rounded_tier"], []],
        "session_id_sum_duration_sec_rounded_tier",
        "duration_sec_rounded_percent_rank_tier",
    )
    .agg(
        F.percentile_approx("duration_sec_rounded", 0.9).alias("duration_sec_rounded_p90"),
        F.size(F.collect_set("page")).alias("num_pages"),
        F.sort_array(F.collect_set("page")).alias("page_list"),
        F.grouping_id().alias("gid"),
    )
)
