from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("browser", "device")
w3 = Window.partitionBy("page").orderBy("device_length")

filtered = (
    df_sessions
    .filter((F.col("started_at") >= F.lit("2024-03-21 13:00:00").cast("timestamp")) & F.col("browser").isNotNull())
    .select(
        "id",
        "device",
        "browser",
        "started_at",
        "country",
        F.length("browser").alias("browser_length"),
        F.when(F.col("browser") == "Edge", 1).otherwise(0).alias("is_edge"),
    )
)

with_latest = (
    filtered
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") <= F.col("s.started_at")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.event_type"), F.col("c.page"))
)

base = (
    with_latest
    .filter(F.col("event_type").isin(["page_view", "search", "purchase"]))
    .select(
        "id",
        "device",
        "browser",
        "started_at",
        "browser_length",
        "page",
        F.length("device").alias("device_length"),
        F.coalesce("browser", F.lit("NONE")).alias("browser_clean"),
    )
)

metrics = (
    base
    .select(
        "*",
        F.min("browser_length").over(w2).alias("browser_min_browser_length"),
        F.percent_rank().over(w3).alias("device_length_percent_rank"),
    )
)

result = (
    metrics
    .select(
        "id",
        "device",
        "browser",
        "browser_length",
        "page",
        "device_length",
        "browser_clean",
        "browser_min_browser_length",
    )
    .orderBy("id", F.col("browser_length").desc())
)
