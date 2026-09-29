from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id", "event_type").orderBy(F.col("duration_sec").desc_nulls_first(), "id")

base = (
    df_web_events
    .filter((F.col("event_time") >= F.lit("2023-10-17").cast("date")) & F.col("page").isNotNull() & F.col("session_id").isin([6, 7, 30]))
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.col("duration_sec").cast("string").alias("duration_sec_num"),
        F.date_format("event_time", "yyyy-MM-dd").alias("event_time_label"),
        F.coalesce("page", F.lit("NONE")).alias("resolved_page"),
    )
)

result = (
    base
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)
