from pyspark.sql import functions as F

exploded = df_app_logs.select("id", "service", F.explode("context").alias("key", "v"))

matched_keys = (
    exploded
    .select("service")
    .subtract(
        df_app_logs
        .filter(F.col("status_code") <= 263)
        .select("service")
    )
)

reconciled = (
    exploded
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.service") == F.col("k.service")), "inner")
    .select("s.*")
)

base = (
    reconciled
    .filter(~F.col("key").isin(["version", "trace"]))
    .withColumn("key_filled", F.coalesce("key", F.lit("N/A")))
)

kept_keys = (
    base
    .select("id")
    .intersectAll(
        df_app_logs
        .filter(F.col("host").isin(["h1", "h3"]))
        .select("id")
    )
)

reconciled_app_logs = (
    base
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

grouped = (
    reconciled_app_logs
    .groupBy("service")
    .agg(
        F.count_if(F.col("service") == "search").alias("search_events"),
        F.sort_array(F.collect_set("key")).alias("key_list"),
        F.count(F.when(F.col("key") == "version", 1)).alias("version_rows"),
    )
)

result = grouped.select("service", "key_list")
