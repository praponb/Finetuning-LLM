from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page", "device").orderBy(F.col("duration_sec_rounded").desc_nulls_last())
w_running = Window.partitionBy("page", "device").orderBy(F.col("duration_sec").asc_nulls_last())
w_part = Window.partitionBy("device").orderBy(F.col("duration_sec").desc())

remaining_keys = (
    df_web_events
    .select("id", "page")
    .intersect(
        df_web_events
        .filter(F.col("session_id").isin([7, 19, 31]))
        .select("id", "page")
    )
)

kept_rows = (
    df_web_events
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.page") == F.col("k.page")), "inner")
    .select("s.*")
)

joined = (
    kept_rows
    .alias("a")
    .join(df_sessions.alias("p"), F.col("a.session_id") == F.col("p.id"), "left")
    .filter(F.hour(F.col("p.started_at")).between(11, 18))
    .select(
        "a.id",
        "a.session_id",
        "a.page",
        "a.duration_sec",
        F.col("p.device"),
        F.col("p.browser"),
        F.col("p.country"),
    )
)

cleaned = (
    joined
    .filter(F.col("page").isin(["/home", "/cart"]))
    .select(
        "*",
        F.round("duration_sec", 1).alias("duration_sec_rounded"),
        F.coalesce("device", F.lit("NONE")).alias("device_filled"),
    )
)

base = (
    cleaned
    .filter(F.col("country").isNotNull())
    .select(
        "id",
        "page",
        "duration_sec",
        "device",
        "duration_sec_rounded",
        "device_filled",
        F.coalesce("country", F.lit("unassigned")).alias("country_filled"),
        F.length("page").alias("page_length"),
    )
)

with_windows = (
    base
    .select(
        "id",
        "duration_sec",
        "device_filled",
        "country_filled",
        "page_length",
        F.rank().over(w).alias("duration_sec_rounded_rank"),
        F.dense_rank().over(w_running).alias("duration_sec_dense_rank"),
        F.dense_rank().over(w_part).alias("duration_sec_dense_rank_2"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("duration_sec") >= 425, "HIGH").otherwise("LOW").alias("duration_sec_tier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.percentile_approx("duration_sec_rounded_rank", 0.95).alias("duration_sec_rounded_rank_p95"),
        F.max("duration_sec_dense_rank").alias("max_duration_sec_dense_rank"),
    )
    .orderBy(F.col("duration_sec_rounded_rank_p95").desc())
)
