from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("is_mobile", "id")
w2 = Window.partitionBy("browser").orderBy("is_mobile", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("browser").orderBy("is_mobile", "id").rowsBetween(-3, -1)

valid = (
    df_sessions
    .filter(F.col("browser").isin(["Chrome", "Firefox", "Safari"]))
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "country",
        F.trim("country").alias("clean_country"),
        F.substring("browser", 1, 2).alias("browser_prefix"),
    )
)

cleaned = (
    valid
    .filter(F.col("browser").isin(["Edge", "Firefox", "Safari"]))
    .withColumn("device_prefix", F.substring("device", 1, 3))
    .withColumn("browser_filled", F.coalesce("browser", F.lit("DIRECT")))
)

cleaned_sessions = (
    cleaned
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "device",
        "browser",
        "country",
        "clean_country",
        "browser_prefix",
        F.concat_ws("-", "browser", "device").alias("browser_device_key"),
        F.lower("device").alias("device_lower"),
        F.coalesce("browser", F.lit("UNKNOWN")).alias("browser_clean"),
    )
)

eligible = (
    cleaned_sessions
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.event_time") >= "2023-10-01"), "left_anti")
)

valid_sessions = (
    eligible
    .filter(F.col("country").isNotNull())
    .select(
        "*",
        F.when(F.col("device") == "mobile", 1).otherwise(0).alias("is_mobile"),
        F.concat_ws("-", "country", "device").alias("country_device_key"),
        F.coalesce("browser", F.lit("N/A")).alias("browser_clean_2"),
    )
)

not_flagged = (
    valid_sessions
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.event_time") >= "2025-10-01"), "left_semi")
)

metrics = (
    not_flagged
    .select(
        "*",
        F.ntile(3).over(w).alias("is_mobile_bucket"),
        F.sum("is_mobile").over(w2).alias("running_is_mobile"),
        F.avg("is_mobile").over(w3).alias("is_mobile_avg_3_prior"),
    )
)

result = (
    metrics
    .rollup("country", "browser")
    .agg(
        F.round(F.sum("running_is_mobile") / F.nullif(F.count(F.when(F.col("browser") == "Chrome", 1)), F.lit(0)), 2).alias("running_is_mobile_per_chrome"),
        F.percentile_approx("running_is_mobile", [0.25, 0.5, 0.75]).alias("running_is_mobile_quartiles"),
        F.sum("is_mobile").alias("total_is_mobile"),
        F.size(F.collect_set("country")).alias("num_countries"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("total_is_mobile").desc())
)
