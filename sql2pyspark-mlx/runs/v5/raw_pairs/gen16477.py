from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("is_chrome", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.orderBy("started_at_hour", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("country").orderBy("is_chrome", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("customer_id").orderBy("is_chrome", "id")
w5 = Window.partitionBy("country").orderBy("started_at_hour", "id").rowsBetween(-6, -1)
w6 = Window.orderBy("is_chrome", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("next_started_at_hour", "id")

cleaned = (
    df_sessions
    .filter((F.col("started_at") > F.lit("2023-07-05 09:00:00").cast("timestamp")) & F.col("browser").isNotNull())
    .select(
        "id",
        "customer_id",
        "browser",
        "country",
        F.hour("started_at").alias("started_at_hour"),
        F.concat_ws("-", "browser", "device").alias("browser_device_key"),
        F.coalesce("browser", F.lit("NONE")).alias("browser_filled"),
    )
)

cleaned_sessions = (
    cleaned
    .filter(F.col("started_at_hour").between(53, 78))
    .select(
        "*",
        F.when(F.col("browser") == "Chrome", 1).otherwise(0).alias("is_chrome"),
        F.substring("country", 1, 2).alias("country_prefix"),
        F.coalesce("browser", F.lit("N/A")).alias("resolved_browser"),
    )
)

with_windows = (
    cleaned_sessions
    .filter(F.col("started_at_hour") > 73)
    .select(
        "id",
        "started_at_hour",
        "browser_filled",
        "is_chrome",
        "country_prefix",
        F.avg("started_at_hour").over(w).alias("started_at_hour_avg_2"),
        F.first("country", ignorenulls=True).over(w2).alias("first_known_country"),
        F.first("browser", ignorenulls=True).over(w3).alias("first_known_browser"),
        F.lead("started_at_hour", 1, 0).over(w4).alias("next_started_at_hour"),
        F.avg("is_chrome").over(w5).alias("is_chrome_avg_6_prior"),
    )
)

metrics = (
    with_windows
    .select(
        "id",
        "browser_filled",
        "is_chrome",
        "first_known_country",
        "first_known_browser",
        F.sum("started_at_hour_avg_2").over(w6).alias("running_sum_started_at_hour_avg_2"),
        F.min("started_at_hour_avg_2").over(w6).alias("running_min_started_at_hour_avg_2"),
        F.max("started_at_hour_avg_2").over(w6).alias("running_max_started_at_hour_avg_2"),
        F.row_number().over(w7).alias("event_seq"),
    )
)

result = (
    metrics
    .rollup(
        F.when(F.col("running_max_started_at_hour_avg_2") >= 66, "PREMIUM").otherwise("STANDARD").alias("running_max_started_at_hour_avg_2_tier"),
        F.when(F.col("is_chrome") >= 76, "LARGE").otherwise("SMALL").alias("is_chrome_tier"),
        F.when(F.col("running_min_started_at_hour_avg_2") >= 70, "LARGE").otherwise("SMALL").alias("running_min_started_at_hour_avg_2_tier"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.countDistinct("id").alias("distinct_ids"),
        F.avg("is_chrome").alias("avg_is_chrome"),
        F.round(F.avg("running_max_started_at_hour_avg_2"), 2).alias("avg_running_max_started_at_hour_avg_2"),
        F.min("running_min_started_at_hour_avg_2").alias("min_running_min_started_at_hour_avg_2"),
    )
    .orderBy(F.col("avg_is_chrome").asc_nulls_last())
)
