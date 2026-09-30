from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("is_checkout", "id")
w2 = Window.partitionBy("event_type")
w3 = Window.partitionBy("event_type", "page").orderBy("duration_sec", "id")
w4 = Window.partitionBy("event_type").orderBy(F.col("seq_no").asc_nulls_first())
w5 = Window.partitionBy("page").orderBy("is_checkout", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_web_events
    .filter(F.col("page").isNotNull())
    .withColumn("event_time_label", F.date_format("event_time", "yyyy-MM"))
)

base = (
    filtered
    .filter((F.col("event_time") > F.to_timestamp(F.lit("2023-09-05 05:00:00"))) & F.col("page").isNotNull())
    .select(
        "*",
        F.when(F.col("page") == "/checkout", 1).otherwise(0).alias("is_checkout"),
        F.coalesce("page", F.lit("NONE")).alias("page_clean"),
    )
)

valid = (
    base
    .filter((F.col("event_type") == "search") & (F.col("event_time") >= F.lit("2023-07-21 08:00:00").cast("timestamp")))
    .select(
        "id",
        "event_type",
        "page",
        "duration_sec",
        "event_time_label",
        "is_checkout",
        "page_clean",
        F.when(F.col("is_checkout") >= 71, "High").when(F.col("is_checkout") >= 33, "Medium").otherwise("Low").alias("is_checkout_band"),
    )
)

with_windows = (
    valid
    .withColumn("prev_is_checkout", F.lag("is_checkout").over(w))
    .withColumn("prev_duration_sec", F.lag("duration_sec").over(w))
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("event_type_stddev_samp_is_checkout", F.stddev_samp("is_checkout").over(w2))
)

scored = (
    with_windows
    .withColumn("prev_seq_no", F.lag("seq_no", 1).over(w3))
    .withColumn("seq_no_dense_rank", F.dense_rank().over(w4))
    .withColumn("event_type_max_prev_duration_sec", F.max("prev_duration_sec").over(w2))
    .withColumn("running_prev_is_checkout", F.sum("prev_is_checkout").over(w5))
)

result = (
    scored
    .select(
        "id",
        "event_type",
        "page",
        "duration_sec",
        "event_time_label",
        "is_checkout",
        "is_checkout_band",
        "prev_is_checkout",
        "prev_duration_sec",
    )
    .orderBy(F.col("prev_is_checkout").desc_nulls_first())
)
