from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy(F.col("duration_sec").desc_nulls_first(), "id")
w2 = Window.partitionBy("session_device", "session_browser").orderBy(F.col("event_time").desc_nulls_last(), "id")
w3 = Window.orderBy(F.col("duration_sec").desc_nulls_last())
w4 = Window.partitionBy("session_id").orderBy(F.col("duration_sec").desc_nulls_last())
w5 = Window.partitionBy("browser").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)

ranked = (
    df_web_events
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

joined = (
    ranked
    .alias("s")
    .join(df_sessions.alias("d"), F.col("s.session_id") == F.col("d.id"), "inner")
    .select("s.*", F.col("d.browser"), F.col("d.device"))
)

joined_web_events = (
    joined
    .alias("s")
    .join(df_sessions.alias("b"), F.col("s.session_id") == F.col("b.id"), "left")
    .select(
        "s.*",
        F.col("b.country"),
        F.col("b.browser").alias("session_browser"),
        F.col("b.device").alias("session_device"),
    )
)

ranked_web_events = (
    joined_web_events
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
)

matched_keys = (
    ranked_web_events
    .select("id")
    .exceptAll(
        df_web_events
        .filter(F.col("page") == "/cart")
        .select("id")
    )
)

kept_rows = (
    ranked_web_events
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

valid = (
    kept_rows
    .filter(F.col("event_time") >= F.lit("2024-02-15").cast("date"))
    .select(
        "*",
        F.when(F.col("session_device").isin(["mobile", "desktop"]), "Priority").otherwise("Other").alias("session_device_group"),
    )
)

metrics = (
    valid
    .select(
        "*",
        F.dense_rank().over(w3).alias("duration_sec_dense_rank"),
        F.percent_rank().over(w4).alias("duration_sec_percent_rank"),
        F.count("duration_sec").over(w5).alias("duration_sec_count_30d"),
    )
)

result = (
    metrics
    .groupBy("session_browser")
    .pivot("device", ["desktop", "tablet"])
    .agg(
        F.avg("duration_sec_percent_rank").alias("value_sum"),
        F.count(F.lit(1)).alias("cnt"),
    )
)
