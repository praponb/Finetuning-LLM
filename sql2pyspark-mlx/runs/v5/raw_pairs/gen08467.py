from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy(F.col("started_at").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())

ranked = (
    df_sessions
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_last = (
    ranked
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.duration_sec"))
)

matched_keys = (
    with_last
    .select("device", "customer_id")
    .intersect(
        df_sessions
        .filter(F.col("customer_id").isNull())
        .select("device", "customer_id")
    )
)

verified = (
    with_last
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.device") == F.col("k.device")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

kept_keys = (
    verified
    .select("browser", "country")
    .subtract(
        df_sessions
        .filter(F.col("started_at") < "2025-01-21")
        .select("browser", "country")
    )
)

kept_rows = (
    verified
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.browser") == F.col("k.browser")) & (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

not_flagged = (
    kept_rows
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.event_time") >= F.col("s.started_at")), "left_semi")
)

filtered = (
    not_flagged
    .filter(F.col("device").isin(["mobile", "desktop", "tablet"]))
    .select(
        "id",
        "customer_id",
        "device",
        "country",
        "duration_sec",
        F.length("country").alias("country_length"),
        F.coalesce("country", F.lit("DIRECT")).alias("country_filled"),
    )
)

result = (
    filtered
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "inner")
    .groupingSets(
        [["segment", "device"], ["segment"], []],
        F.col("c.segment").alias("segment"),
        F.col("src.device").alias("device"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.max(F.col("src.country_length")).alias("max_country_length"),
        F.min(F.col("src.duration_sec")).alias("min_duration_sec"),
    )
)
