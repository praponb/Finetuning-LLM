from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())
w2 = Window.orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("web_event_page", "browser").orderBy(F.col("started_at").desc(), "id")
w4 = Window.partitionBy("device").orderBy("started_at", "id")
w5 = Window.partitionBy("customer_id", "browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("web_event_page", "device").orderBy(F.col("started_at").desc(), "id")
w7 = Window.partitionBy("browser").orderBy(F.col("event_seq").desc_nulls_first(), "id")

base = (
    df_sessions
    .filter(F.col("started_at") > F.lit("2024-04-19 11:00:00").cast("timestamp"))
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        F.upper("country").alias("country_upper"),
        F.concat("device", F.lit(", "), "browser").alias("device_browser"),
    )
)

with_latest = (
    base
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.page").alias("web_event_page"))
)

with_windows = (
    with_latest
    .select(
        "*",
        F.first("web_event_page", ignorenulls=True).over(w2).alias("first_known_web_event_page"),
        F.row_number().over(w3).alias("seq_no"),
        F.row_number().over(w4).alias("row_seq"),
        F.last("web_event_page", ignorenulls=True).over(w5).alias("last_known_web_event_page"),
        F.row_number().over(w6).alias("event_seq"),
    )
)

with_last = (
    with_windows
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") <= F.col("s.started_at")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.event_time"), F.col("c.event_type"))
)

ranked = (
    with_last
    .withColumn("latest_rank", F.row_number().over(w7))
    .filter(F.col("latest_rank") == 1)
)

kept = (
    ranked
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") < F.col("s.started_at")), "left_semi")
)

result = (
    kept
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "device_browser",
        "first_known_web_event_page",
        "seq_no",
        "row_seq",
    )
    .orderBy(F.col("row_seq").asc_nulls_first())
)
