from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page", "event_type")
w2 = Window.orderBy("duration_sec", "id")
w3 = Window.partitionBy("event_type").orderBy(F.col("page_length").desc())
w4 = Window.partitionBy("event_type")
w5 = Window.partitionBy("event_type").orderBy(F.col("page_length").desc(), "id")
w6 = Window.orderBy(F.col("event_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w7 = Window.orderBy(F.col("page_length_dense_rank").desc())

cleaned = (
    df_web_events
    .filter((F.col("event_time") >= F.lit("2023-07-09 18:00:00").cast("timestamp")) & F.col("page").isNotNull())
    .withColumn("page_length", F.length("page"))
    .withColumn("page_prefix", F.substring("page", 1, 3))
)

metrics = (
    cleaned
    .filter(F.col("session_id").isin([10, 25]))
    .select(
        "*",
        F.min("duration_sec").over(w).alias("page_min_duration_sec"),
        F.lag("page_length", 1).over(w2).alias("prev_page_length"),
        F.dense_rank().over(w3).alias("page_length_dense_rank"),
    )
)

metrics_web_events = (
    metrics
    .select(
        "id",
        "session_id",
        "page",
        "event_time",
        "duration_sec",
        "page_length",
        "page_prefix",
        "prev_page_length",
        "page_length_dense_rank",
        F.count("prev_page_length").over(w4).alias("event_type_count_prev_page_length"),
        F.row_number().over(w5).alias("event_seq"),
    )
)

kept_keys = (
    metrics_web_events
    .select("id", "page")
    .intersectAll(
        df_web_events
        .filter(F.col("page") == "/product")
        .select("id", "page")
    )
)

kept_rows = (
    metrics_web_events
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.page") == F.col("k.page")), "inner")
    .select("s.*")
)

with_dims = (
    kept_rows
    .alias("a")
    .join(df_sessions.alias("d"), F.col("a.session_id") == F.col("d.id"), "inner")
    .select("a.*", F.col("d.device"), F.col("d.country"))
)

enriched = (
    with_dims
    .select(
        "id",
        "page",
        "prev_page_length",
        "device",
        "country",
        F.count("event_seq").over(w6).alias("event_seq_count_7d"),
        F.rank().over(w7).alias("page_length_dense_rank_rank"),
    )
)

result = (
    enriched
    .groupingSets([["device", "page"], ["device"], []], "device", "page")
    .agg(
        F.round(F.avg("event_seq_count_7d"), 2).alias("avg_event_seq_count_7d"),
        F.percentile_approx("event_seq_count_7d", [0.25, 0.5, 0.75]).alias("event_seq_count_7d_quartiles"),
        F.percentile_approx("page_length_dense_rank_rank", [0.25, 0.5, 0.75]).alias("page_length_dense_rank_rank_quartiles"),
        F.count(F.when(F.col("page") == "/product", 1)).alias("product_count"),
    )
)
