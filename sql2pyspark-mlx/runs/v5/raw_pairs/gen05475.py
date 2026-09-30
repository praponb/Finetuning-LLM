from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page", "event_type").orderBy(F.col("duration_sec").asc_nulls_first(), "id")
w2 = Window.partitionBy("page").orderBy("duration_sec_filled", "id")
w3 = Window.partitionBy("event_type", "page")

cleaned = (
    df_web_events
    .filter((F.col("event_time") >= F.to_date(F.lit("2023-08-29"))) & F.col("page").isNotNull())
    .select(
        "id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.coalesce("duration_sec", F.lit(0)).alias("duration_sec_filled"),
        F.concat("event_type", F.lit(" - "), "page").alias("event_type_page"),
    )
)

deduped = (
    cleaned
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

with_windows = (
    deduped
    .withColumn("duration_sec_filled_bucket", F.ntile(3).over(w2))
    .withColumn("event_type_stddev_samp_duration_sec_filled", F.stddev_samp("duration_sec_filled").over(w3))
)

filtered = (
    with_windows
    .filter(F.col("page").isNotNull())
    .select(
        "id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        "duration_sec_filled",
        "event_type_page",
        "dedup_rank",
        "duration_sec_filled_bucket",
        "event_type_stddev_samp_duration_sec_filled",
        F.coalesce("duration_sec_filled", F.lit(0)).alias("duration_sec_filled_filled"),
        F.coalesce("page", F.lit("NONE")).alias("page_clean"),
    )
)

result = (
    filtered
    .groupBy("page", F.date_trunc("YEAR", "event_time").alias("year_start"))
    .agg(
        ((F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))) / 60).alias("span_minutes"),
        F.count(F.when(F.col("page") == "/cart", 1)).alias("cart_count"),
        F.sum("duration_sec_filled").alias("total_duration_sec_filled"),
    )
    .filter(F.col("total_duration_sec_filled") > 0)
)
