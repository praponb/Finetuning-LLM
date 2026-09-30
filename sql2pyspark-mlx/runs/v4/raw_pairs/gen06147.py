from pyspark.sql import functions as F

matched_keys = (
    df_sessions
    .select("customer_id", "id")
    .exceptAll(
        df_sessions
        .filter(F.hour("started_at").between(9, 15))
        .select("customer_id", "id")
    )
)

verified = (
    df_sessions
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .withColumn("year_start", F.date_trunc("YEAR", "started_at"))
    .withColumn("activity_month", F.date_trunc("MONTH", "started_at"))
    .cube("year_start", "browser", "activity_month")
    .agg(
        F.count(F.when(F.col("country") == "DE", 1)).alias("de_count"),
        F.max("started_at").alias("ended_at"),
        F.count_if(F.col("browser") == "Safari").alias("safari_events"),
        F.sort_array(F.collect_set("browser")).alias("browser_list"),
        F.min("started_at").alias("first_started_at"),
        F.count(F.when(F.col("device") == "tablet", 1)).alias("tablet_count"),
        F.grouping("year_start").alias("grouping_id"),
    )
    .orderBy(F.col("year_start").asc_nulls_first())
)
