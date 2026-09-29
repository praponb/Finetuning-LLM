from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("browser").orderBy("started_at_period", "id")
w3 = Window.partitionBy("browser").orderBy("started_at_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_sessions
    .filter(~F.col("browser").eqNullSafe(F.lit("Chrome")))
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        F.date_trunc("month", "started_at").alias("started_at_period"),
        F.concat_ws("/", "device", "country").alias("device_country_key"),
        F.coalesce("browser", F.lit("NONE")).alias("browser_filled"),
    )
)

with_last = (
    valid
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.duration_sec").alias("web_event_duration_sec"))
)

cleaned = (
    with_last
    .filter(F.col("web_event_duration_sec") == 286)
    .select(
        "id",
        "device",
        "browser",
        "started_at",
        "country",
        "started_at_period",
        "device_country_key",
        "browser_filled",
        "web_event_duration_sec",
        F.when(F.col("device").isin(["tablet", "desktop"]), "Priority").otherwise("Other").alias("device_group"),
    )
)

gaps = (
    cleaned
    .withColumn("is_new_session", F.when(F.lag("started_at_period").over(w2).isNull() | (F.col("started_at_period") > F.lag("started_at_period").over(w2) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

islands = gaps.withColumn("session_no", F.sum("is_new_session").over(w3))

kept_keys = (
    islands
    .select("country", "browser")
    .subtract(
        df_sessions
        .filter(~F.col("device").isin(["mobile", "desktop"]))
        .select("country", "browser")
    )
)

verified = (
    islands
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.country") == F.col("k.country")) & (F.col("s.browser") == F.col("k.browser")), "inner")
    .select("s.*")
)

filtered = (
    verified
    .filter(F.col("session_no") == 8)
    .select(
        "id",
        "device",
        "browser",
        "started_at",
        "started_at_period",
        "device_country_key",
        "web_event_duration_sec",
        "device_group",
        "is_new_session",
        "session_no",
        F.coalesce("country", F.lit("N/A")).alias("country_filled"),
        F.length("browser").alias("browser_length"),
        F.coalesce("country", F.lit("NONE")).alias("country_clean"),
    )
)

not_flagged = (
    filtered
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.event_time") < F.col("s.started_at")), "left_semi")
)

result = (
    not_flagged
    .withColumn("quarter_start", F.date_trunc("QUARTER", "started_at_period"))
    .groupingSets(
        [["device", "quarter_start"], ["device"], []],
        "device",
        "quarter_start",
    )
    .agg(
        F.size(F.collect_set("browser")).alias("num_browsers"),
        F.countDistinct("device").alias("distinct_devices"),
    )
)
