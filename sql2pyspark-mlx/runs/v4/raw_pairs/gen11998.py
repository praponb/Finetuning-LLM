from pyspark.sql import functions as F

result = (
    df_sessions
    .groupBy(F.date_trunc("WEEK", "started_at").alias("week_start"))
    .agg(
        F.countDistinct("device").alias("distinct_devices"),
        F.size(F.collect_set("browser")).alias("num_browsers"),
        F.max("started_at").alias("last_started_at"),
        F.count("*").alias("num_rows"),
        F.count_if(F.col("device") == "desktop").alias("desktop_events"),
    )
    .orderBy(F.col("num_rows").asc_nulls_first())
)
