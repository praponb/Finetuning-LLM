from pyspark.sql import functions as F

matched_keys = (
    df_sessions
    .select("country", "browser")
    .subtract(
        df_sessions
        .filter(F.col("device") == "tablet")
        .select("country", "browser")
    )
)

verified = (
    df_sessions
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.country") == F.col("k.country")) & (F.col("s.browser") == F.col("k.browser")), "inner")
    .select("s.*")
)

result = (
    verified
    .withColumn("week_start", F.date_trunc("WEEK", "started_at"))
    .withColumn("quarter_start", F.date_trunc("QUARTER", "started_at"))
    .groupingSets(
        [["week_start", "quarter_start"], ["week_start"]],
        "week_start",
        "quarter_start",
    )
    .agg(
        F.count(F.when(F.col("device") == "desktop", 1)).alias("desktop_count"),
        F.min("started_at").alias("first_seen"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.grouping_id().alias("gid"),
    )
)
