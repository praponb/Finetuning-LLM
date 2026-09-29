from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at_hour", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("started_at", "id").rowsBetween(-5, -1)
w3 = Window.partitionBy("device")
w4 = Window.partitionBy("country").orderBy("started_at_hour", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("country").orderBy("device_avg_started_at_hour", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("browser", "device").orderBy("started_at_hour_avg_5_prior", "id")
w7 = Window.partitionBy("browser").orderBy("started_at_hour_avg_5_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_sessions
    .filter(F.col("browser") == "Firefox")
    .select(
        "*",
        F.hour("started_at").alias("started_at_hour"),
        F.concat_ws("-", "device", "browser").alias("device_browser_key"),
        F.coalesce("country", F.lit("OTHER")).alias("country_filled"),
    )
)

with_windows = (
    cleaned
    .select(
        "id",
        "device",
        "browser",
        "country",
        "device_browser_key",
        F.sum("started_at_hour").over(w).alias("cumulative_started_at_hour"),
        F.avg("started_at_hour").over(w2).alias("started_at_hour_avg_5_prior"),
        F.avg("started_at_hour").over(w3).alias("device_avg_started_at_hour"),
        F.last("country", ignorenulls=True).over(w4).alias("last_known_country"),
    )
)

enriched = (
    with_windows
    .select(
        "*",
        F.sum("started_at_hour_avg_5_prior").over(w5).alias("running_started_at_hour_avg_5_prior"),
        F.lead("cumulative_started_at_hour", 1).over(w6).alias("next_cumulative_started_at_hour"),
        F.sum("started_at_hour_avg_5_prior").over(w7).alias("running_started_at_hour_avg_5_prior_2"),
    )
)

eligible = (
    enriched
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.duration_sec") < 279), "left_semi")
)

valid = (
    eligible
    .filter(F.col("device_avg_started_at_hour").between(28, 30))
    .select(
        "id",
        "browser",
        "country",
        "device_browser_key",
        "cumulative_started_at_hour",
        "last_known_country",
        "running_started_at_hour_avg_5_prior",
        "running_started_at_hour_avg_5_prior_2",
        F.coalesce("device_avg_started_at_hour", F.lit(0)).alias("device_avg_started_at_hour_filled"),
        F.coalesce("country", F.lit("NONE")).alias("country_filled"),
    )
)

result = (
    valid
    .groupBy("browser")
    .agg(
        F.countDistinct("browser").alias("distinct_browsers"),
        F.round(F.avg("running_started_at_hour_avg_5_prior"), 2).alias("avg_running_started_at_hour_avg_5_prior"),
        F.round(F.avg("cumulative_started_at_hour"), 2).alias("avg_cumulative_started_at_hour"),
        F.count_if(F.col("browser") == "Edge").alias("edge_events"),
    )
)
