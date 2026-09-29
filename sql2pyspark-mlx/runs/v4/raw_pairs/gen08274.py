from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy(F.col("duration_sec").asc_nulls_first(), "id")
w2 = Window.partitionBy("page").orderBy(F.col("event_time").desc())
w3 = Window.partitionBy("session_browser").orderBy(F.col("duration_sec").desc(), "id")
w4 = Window.partitionBy("device").orderBy(F.col("duration_sec").desc_nulls_last())
w5 = Window.partitionBy("event_type", "device").orderBy(F.col("dedup_rank").desc(), "id")
w6 = Window.orderBy("event_time", "id").rowsBetween(-2, Window.currentRow)
w7 = Window.orderBy("duration_sec", "id")
w8 = Window.partitionBy("event_type").orderBy(F.col("duration_sec").desc_nulls_first(), "id")

joined = (
    df_web_events
    .alias("a")
    .join(df_sessions.alias("p"), F.col("a.session_id") == F.col("p.id"), "inner")
    .select("a.*", F.col("p.device"), F.col("p.browser"))
)

ranked = (
    joined
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

latest = (
    ranked
    .withColumn("dedup_rank", F.rank().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

with_dims = (
    latest
    .alias("src")
    .join(df_sessions.alias("p"), F.col("src.session_id") == F.col("p.id"), "left")
    .select(
        "src.*",
        F.col("p.browser").alias("session_browser"),
        F.col("p.device").alias("session_device"),
    )
)

with_windows = (
    with_dims
    .withColumn("duration_sec_bucket", F.ntile(10).over(w3))
    .withColumn("duration_sec_cume_dist", F.cume_dist().over(w4))
    .withColumn("seq_no", F.row_number().over(w5))
    .withColumn("duration_sec_avg_2", F.avg("duration_sec").over(w6))
    .withColumn("duration_sec_bucket_2", F.ntile(10).over(w7))
)

latest_web_events = (
    with_windows
    .withColumn("rank_in_group", F.row_number().over(w8))
    .filter(F.col("rank_in_group") == 1)
)

base = (
    latest_web_events
    .filter((F.col("event_time") > F.lit("2023-12-28 06:00:00").cast("timestamp")) & F.col("device").isNotNull())
    .withColumn("duration_sec_bucket_2_times_duration_sec_avg_2", (F.col("duration_sec_bucket_2") * F.col("duration_sec_avg_2")))
    .withColumn("resolved_page", F.coalesce("page", F.lit("NONE")))
)

result = (
    base
    .groupBy("browser")
    .pivot("page", ["/product", "/checkout", "/cart"])
    .agg(F.sum("duration_sec_bucket_2").alias("value_sum"))
    .select(
        "browser",
        F.col("/product").alias("product"),
        F.col("/checkout").alias("checkout"),
        F.col("/cart").alias("cart"),
    )
)
