from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("started_at_date", "id")
w2 = Window.partitionBy("country").orderBy(F.col("started_at").desc_nulls_last())
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())

valid = (
    df_sessions
    .filter(F.col("started_at") >= "2024-05-01")
    .withColumn("device_prefix", F.substring("device", 1, 2))
    .withColumn("started_at_date", F.to_date("started_at"))
    .withColumn("country_clean", F.coalesce("country", F.lit("DIRECT")))
)

ranked = (
    valid
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

filtered = (
    ranked
    .filter((F.col("started_at") >= F.to_date(F.lit("2024-01-05"))) & F.col("country").isNotNull())
    .withColumn("started_at_date_2", F.to_date("started_at"))
    .withColumn("browser_device", F.concat("browser", F.lit(" - "), "device"))
)

latest = (
    filtered
    .withColumn("rn", F.rank().over(w2))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_latest = (
    latest
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.event_time"), F.col("c.event_type"))
)

not_flagged = (
    with_latest
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.event_type") == "purchase"), "left_anti")
)

kept_keys = (
    not_flagged
    .select("device")
    .subtract(
        df_sessions
        .filter(F.col("country").isin(["DE", "TH", "UK"]))
        .select("device")
    )
)

kept_rows = (
    not_flagged
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.device") == F.col("k.device")), "inner")
    .select("s.*")
)

daily = (
    kept_rows
    .withColumn("activity_month", F.date_trunc("MONTH", "started_at_date_2"))
    .groupingSets(
        [["activity_month", "event_type"], ["activity_month"], []],
        "activity_month",
        "event_type",
    )
    .agg(
        F.size(F.collect_set("device")).alias("num_devices"),
        F.max("started_at").alias("last_seen"),
        F.min("started_at").alias("started_at"),
        F.countDistinct("browser").alias("distinct_browsers"),
        F.grouping("activity_month").alias("grouping_id"),
    )
)

result = (
    daily
    .select(
        "activity_month",
        "num_devices",
        "last_seen",
        "started_at",
        "distinct_browsers",
        "grouping_id",
    )
    .orderBy("activity_month")
)
