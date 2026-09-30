from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy(F.col("duration_sec").desc_nulls_last(), "id")
w2 = Window.partitionBy("browser", "device").orderBy(F.col("device_length").desc())
w_part = Window.partitionBy("session_id").orderBy("device_length", "id")
w4 = Window.orderBy("device_length", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.orderBy(F.col("device_length").desc(), "id")

latest = (
    df_web_events
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

joined = (
    latest
    .alias("a")
    .join(df_sessions.alias("p"), F.col("a.session_id") == F.col("p.id"), "left")
    .select(
        "a.id",
        "a.session_id",
        F.col("p.device"),
        F.col("p.country"),
        F.col("p.browser"),
    )
)

base = (
    joined
    .filter(F.col("country").isNull())
    .select(
        "id",
        "session_id",
        "device",
        "country",
        "browser",
        F.length("device").alias("device_length"),
    )
)

deduped = (
    base
    .withColumn("row_num", F.rank().over(w2))
    .filter(F.col("row_num") == 1)
    .select("id", "session_id", "device_length")
)

with_windows = (
    deduped
    .filter(F.col("device_length") <= 29)
    .select(
        "*",
        F.ntile(3).over(w_part).alias("device_length_bucket"),
        F.sum("device_length").over(w4).alias("cumulative_device_length"),
        F.lead("device_length", 1).over(w_part).alias("next_device_length"),
        F.row_number().over(w_part).alias("row_seq"),
        F.ntile(3).over(w_hist).alias("device_length_bucket_2"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "session_id",
        "cumulative_device_length",
        "row_seq",
        "device_length_bucket_2",
    )
    .orderBy(F.col("id").asc_nulls_last())
)
