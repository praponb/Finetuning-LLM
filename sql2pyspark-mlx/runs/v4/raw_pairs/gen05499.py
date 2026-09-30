from pyspark.sql import functions as F

base = (
    df_sessions
    .filter((F.col("started_at") >= F.lit("2024-01-07 03:00:00").cast("timestamp")) & F.col("browser").isNotNull() & F.col("country").isin(["TH", "JP"]))
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        F.concat_ws("/", "browser", "country").alias("browser_country_key"),
        F.coalesce("browser", F.lit("Unknown")).alias("browser_filled"),
        F.coalesce("browser", F.lit("N/A")).alias("resolved_browser"),
    )
)

result = (
    base
    .groupBy("customer_id")
    .agg(
        F.sort_array(F.collect_set("device")).alias("device_list"),
        F.size(F.collect_set("browser")).alias("num_browsers"),
        F.count(F.when(F.col("browser") == "Safari", 1)).alias("safari_rows"),
        F.size(F.collect_set("browser")).alias("num_browsers_2"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count(F.when(F.col("browser") == "Safari", 1)).alias("safari_rows_2"),
    )
)
