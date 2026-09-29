from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())

filtered = (
    df_sessions
    .filter(F.col("customer_id").isNotNull() & (~F.col("browser").eqNullSafe(F.lit("Firefox"))))
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "country",
        F.when(F.col("browser") == "Firefox", 1).otherwise(0).alias("is_firefox"),
        F.when(F.col("browser").isin(["Safari", "Firefox"]), "Tier 1").otherwise("Other").alias("browser_group"),
        F.coalesce("browser", F.lit("NONE")).alias("browser_clean"),
    )
)

filtered_sessions = (
    filtered
    .filter(F.col("browser").isNotNull())
    .withColumn("is_firefox_whole", F.floor("is_firefox"))
    .withColumn("is_firefox_band", F.when(F.col("is_firefox") >= 79, "High").when(F.col("is_firefox") >= 23, "Medium").otherwise("Low"))
    .withColumn("resolved_browser", F.coalesce("browser", F.lit("UNKNOWN")))
)

joined = (
    filtered_sessions
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id") == F.col("b.id"), "left")
    .select(
        "e.id",
        "e.customer_id",
        "e.is_firefox",
        "e.is_firefox_band",
        "e.resolved_browser",
        F.col("b.city"),
        F.col("b.name"),
        F.col("b.country").alias("customer_country"),
    )
)

latest_match = (
    joined
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.is_firefox",
        "s.is_firefox_band",
        "s.resolved_browser",
        "s.customer_country",
        F.col("c.event_type").alias("web_event_event_type"),
    )
)

with_latest = (
    latest_match
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.is_firefox",
        "s.is_firefox_band",
        "s.customer_country",
        F.col("c.event_type"),
        F.col("c.duration_sec"),
    )
)

result = (
    with_latest
    .select("id", "is_firefox", "is_firefox_band", "customer_country", "event_type")
)
