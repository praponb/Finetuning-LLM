from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("page_length").desc_nulls_last())
w2 = Window.partitionBy("event_type").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w3 = Window.partitionBy("event_type").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w4 = Window.partitionBy("event_type", "page").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)

valid = (
    df_web_events
    .filter(F.col("event_time") >= F.lit("2023-06-28").cast("date"))
    .withColumn("duration_sec_filled", F.coalesce("duration_sec", F.lit(0)))
)

matched_keys = (
    valid
    .select("session_id")
    .subtract(
        df_web_events
        .filter(F.col("duration_sec") > 141)
        .select("session_id")
    )
)

kept_rows = (
    valid
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.session_id") == F.col("k.session_id")), "inner")
    .select("s.*")
)

filtered = (
    kept_rows
    .filter(F.col("page").isNotNull())
    .select(
        "id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        "duration_sec_filled",
        F.length("page").alias("page_length"),
    )
)

metrics = (
    filtered
    .withColumn("page_length_cume_dist", F.cume_dist().over(w))
    .withColumn("duration_sec_filled_sum_30d", F.sum("duration_sec_filled").over(w2))
)

enriched = (
    metrics
    .select(
        "*",
        F.sum("page_length_cume_dist").over(w3).alias("page_length_cume_dist_sum_7d"),
        F.sum("duration_sec").over(w4).alias("duration_sec_sum_30d"),
    )
)

result = (
    enriched
    .groupBy("event_type")
    .agg(
        F.count_if(F.col("page") == "/search").alias("search_events"),
        F.round(F.sum("page_length_cume_dist_sum_7d") / F.nullif(F.count(F.when(F.col("event_type") == "add_to_cart", 1)), F.lit(0)), 2).alias("page_length_cume_dist_sum_7d_per_add_to_cart"),
        F.round(F.avg("duration_sec_sum_30d"), 2).alias("avg_duration_sec_sum_30d"),
    )
    .filter(F.col("search_events") < 1)
    .orderBy("search_events")
)
