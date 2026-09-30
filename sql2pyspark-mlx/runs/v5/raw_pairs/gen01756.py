from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy(F.col("duration_sec").desc())
w2 = Window.partitionBy("event_type").orderBy(F.col("event_time").desc(), "id")
w3 = Window.partitionBy("event_type").orderBy("event_time", "id")

filtered = (
    df_web_events
    .filter((F.col("event_time") >= F.lit("2023-04-17").cast("date")) & (~F.col("page").eqNullSafe(F.lit("/cart"))))
    .select(
        "id",
        "event_type",
        "event_time",
        "duration_sec",
        F.hour("event_time").alias("event_time_hour"),
    )
)

cleaned = (
    filtered
    .filter(F.col("event_time") >= F.lit("2023-05-30 08:00:00").cast("timestamp"))
    .withColumn("event_time_label", F.date_format("event_time", "yyyy-MM-dd"))
)

ranked = (
    cleaned
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "event_type", "event_time")
)

with_windows = (
    ranked
    .select(
        "id",
        "event_type",
        "event_time",
        F.row_number().over(w2).alias("seq_no"),
        F.row_number().over(w3).alias("row_seq"),
        F.row_number().over(w3).alias("event_seq"),
        F.row_number().over(w3).alias("event_seq_2"),
        F.row_number().over(w2).alias("seq_no_2"),
    )
)

kept_keys = (
    with_windows
    .select("event_type", "id")
    .subtract(
        df_web_events
        .filter(F.col("page").isin(["/cart", "/product"]))
        .select("event_type", "id")
    )
)

kept_rows = (
    with_windows
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.event_type") == F.col("k.event_type")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select("id", "event_time", "seq_no", "row_seq", "event_seq", "event_seq_2")
    .orderBy(F.col("event_time").desc_nulls_first())
)
