from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("device_length", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("country").orderBy(F.col("started_at").desc(), "id")
w3 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("browser").orderBy("device_length", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("browser").orderBy("started_at", "id")

base = (
    df_sessions
    .filter(F.col("country").isNotNull() & F.col("browser").isin(["Safari", "Edge"]))
    .select(
        "id",
        "browser",
        "started_at",
        "country",
        F.coalesce("country", F.lit("Unknown")).alias("country_filled"),
        F.length("device").alias("device_length"),
        F.coalesce("country", F.lit("DIRECT")).alias("country_filled_2"),
    )
)

metrics = (
    base
    .filter(F.to_date("started_at") < "2023-10-23")
    .select(
        "*",
        F.first("country", ignorenulls=True).over(w).alias("first_known_country"),
        F.row_number().over(w2).alias("event_seq"),
        F.last("country", ignorenulls=True).over(w3).alias("last_known_country"),
        F.last("device_length", ignorenulls=True).over(w4).alias("last_known_device_length"),
        F.last("country", ignorenulls=True).over(w5).alias("last_known_country_2"),
    )
)

flagged = (
    metrics
    .withColumn("is_new_burst", F.when(F.lag("started_at").over(w6).isNull() | (F.col("started_at") > F.lag("started_at").over(w6) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessions_sessions = flagged.withColumn("session_no", F.sum("is_new_burst").over(w5))

matched_keys = (
    sessions_sessions
    .select("country")
    .subtract(
        df_sessions
        .filter(F.col("browser") == "Safari")
        .select("country")
    )
)

reconciled = (
    sessions_sessions
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

eligible = (
    reconciled
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") < F.col("s.started_at")), "left_anti")
)

result = (
    eligible
    .select(
        "id",
        "browser",
        "started_at",
        "country_filled",
        "device_length",
        "event_seq",
        "last_known_country",
        "last_known_device_length",
        "is_new_burst",
        F.when(F.col("browser").rlike("^(Chrome|Edge)$"), "GROUP_A").when(F.col("browser").rlike("^(Safari|Firefox)$"), "CORE").otherwise("PRIORITY").alias("browser_zone"),
    )
)
