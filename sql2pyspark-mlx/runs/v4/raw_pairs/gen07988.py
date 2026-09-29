from pyspark.sql import functions as F

matched_keys = (
    df_sessions
    .select("device")
    .intersectAll(
        df_sessions
        .filter(F.col("browser") == "Firefox")
        .select("device")
    )
)

reconciled = (
    df_sessions
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.device") == F.col("k.device")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy("browser")
    .agg(
        F.count(F.when(F.col("country") == "SG", 1)).alias("sg_rows"),
        F.count_if(F.col("browser") == "Chrome").alias("chrome_events"),
        F.sort_array(F.collect_set("browser")).alias("browser_list"),
        F.max("started_at").alias("ended_at"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
    )
)
