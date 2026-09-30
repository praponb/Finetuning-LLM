from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())

valid = (
    df_sessions
    .filter((F.col("started_at") >= F.lit("2023-09-17").cast("date")) & F.col("browser").isNotNull())
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        F.concat("device", F.lit("/"), "country").alias("device_country"),
    )
)

latest_match = (
    valid
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.browser",
        "s.device_country",
        F.col("c.duration_sec"),
        F.col("c.page").alias("web_event_page"),
    )
)

eligible = (
    latest_match
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.session_id").isin([6, 32])), "left_anti")
)

cleaned = (
    eligible
    .filter(F.col("web_event_page").isNotNull())
    .select(
        "*",
        F.round((F.col("duration_sec") * 1.07), 2).alias("adjusted_duration_sec"),
    )
)

result = (
    cleaned
    .groupBy(
        "browser",
        F.when(F.col("duration_sec") >= 480, "LARGE").otherwise("SMALL").alias("duration_sec_tier"),
    )
    .agg(
        F.round(F.avg("adjusted_duration_sec"), 2).alias("avg_adjusted_duration_sec"),
        F.count("*").alias("event_count"),
    )
    .orderBy("browser")
)
