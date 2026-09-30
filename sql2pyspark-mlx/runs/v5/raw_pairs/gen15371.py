from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("started_at").desc(), "id")
w2 = Window.partitionBy("device").orderBy(F.col("started_at").desc(), "id")
w3 = Window.partitionBy("country").orderBy(F.col("started_at").desc(), "id")
w4 = Window.partitionBy("country", "device").orderBy("started_at", "id")

with_windows = (
    df_sessions
    .filter(F.col("device") == "desktop")
    .select(
        "id",
        "device",
        "browser",
        "started_at",
        "country",
        F.row_number().over(w).alias("event_seq"),
        F.row_number().over(w2).alias("row_seq"),
        F.row_number().over(w3).alias("row_seq_2"),
        F.row_number().over(w4).alias("row_seq_3"),
    )
)

result = (
    with_windows
    .groupBy("device")
    .agg(
        F.size(F.collect_set("device")).alias("num_devices"),
        F.round(F.avg("row_seq_2"), 2).alias("avg_row_seq_2"),
        F.percentile_approx("event_seq", [0.25, 0.5, 0.75]).alias("event_seq_quartiles"),
        F.sort_array(F.collect_set("device")).alias("device_list"),
        F.size(F.collect_set("country")).alias("num_countries"),
        F.count_if(F.col("device") == "desktop").alias("desktop_events"),
    )
    .filter(F.col("num_devices") > 1)
    .orderBy("num_devices")
)
