from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("event_type").orderBy("duration_sec_filled", "id")
w_part = Window.partitionBy("device").orderBy("duration_sec_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("session_id").orderBy(F.col("duration_sec_filled").desc(), "id")

cleaned = (
    df_web_events
    .filter((F.to_date("event_time") >= "2023-06-01") & (F.col("event_time") >= F.to_date(F.lit("2023-04-21"))))
    .select("*", F.coalesce("duration_sec", F.lit(0)).alias("duration_sec_filled"))
)

joined = (
    cleaned
    .alias("s")
    .join(df_sessions.alias("p2"), F.col("s.session_id") == F.col("p2.id"), "inner")
    .select("s.*", F.col("p2.device"), F.col("p2.country"))
)

scored = (
    joined
    .withColumn("next_duration_sec", F.lead("duration_sec").over(w_recent))
    .withColumn("prev_duration_sec_filled", F.lag("duration_sec_filled").over(w_recent))
    .withColumn("seq_no", F.row_number().over(w_recent))
)

scored_web_events = (
    scored
    .filter(F.col("country") == "TH")
    .select(
        "id",
        "event_type",
        "page",
        "duration_sec_filled",
        "country",
        "next_duration_sec",
        F.sum("prev_duration_sec_filled").over(w_part).alias("running_sum_prev_duration_sec_filled"),
        F.avg("prev_duration_sec_filled").over(w_part).alias("running_avg_prev_duration_sec_filled"),
        F.count("prev_duration_sec_filled").over(w_part).alias("running_count_prev_duration_sec_filled"),
        F.ntile(5).over(w3).alias("duration_sec_filled_bucket"),
    )
)

filtered = (
    scored_web_events
    .filter(F.col("running_count_prev_duration_sec_filled").between(14, 15))
    .select(
        "id",
        "event_type",
        "page",
        "country",
        "next_duration_sec",
        "running_sum_prev_duration_sec_filled",
        "running_avg_prev_duration_sec_filled",
        "running_count_prev_duration_sec_filled",
        "duration_sec_filled_bucket",
        F.length("country").alias("country_length"),
    )
)

valid = (
    filtered
    .filter(F.col("country").isNotNull() & (F.col("running_count_prev_duration_sec_filled") == 16))
    .select("*", F.round("country_length", 2).alias("country_length_rounded"))
)

totals = (
    valid
    .groupBy("country")
    .agg(
        F.round(F.sum("country_length_rounded") / F.nullif(F.count(F.when(F.col("page") == "/search", 1)), F.lit(0)), 2).alias("country_length_rounded_per_search"),
        F.countDistinct("page").alias("distinct_pages"),
        F.max(F.when(F.col("country") == "UK", F.col("country_length_rounded"))).alias("uk_max_country_length_rounded"),
    )
)

result = (
    totals
    .select(
        "country",
        "country_length_rounded_per_search",
        "distinct_pages",
        "uk_max_country_length_rounded",
    )
)
