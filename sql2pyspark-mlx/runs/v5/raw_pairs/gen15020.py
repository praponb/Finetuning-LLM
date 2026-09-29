from pyspark.sql import functions as F

kept_keys = (
    df_sessions
    .select("country")
    .exceptAll(
        df_sessions
        .filter(F.col("customer_id").isin([13, 23, 29, 36]))
        .select("country")
    )
)

kept_rows = (
    df_sessions
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy("browser", F.date_trunc("QUARTER", "started_at").alias("quarter_start"))
    .agg(
        F.count_if(F.col("country") == "DE").alias("de_events"),
        F.max("started_at").alias("ended_at"),
        F.count(F.when(F.col("browser") == "Chrome", 1)).alias("chrome_rows"),
        F.count(F.when(F.col("country") == "TH", 1)).alias("th_count"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("de_events") >= 1)
)
