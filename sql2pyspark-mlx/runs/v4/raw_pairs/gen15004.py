from pyspark.sql import functions as F

result = (
    df_sessions
    .groupBy(
        F.date_trunc("YEAR", "started_at").alias("year_start"),
        F.date_trunc("WEEK", "started_at").alias("week_start"),
    )
    .agg(
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.count(F.when(F.col("browser") == "Firefox", 1)).alias("firefox_rows"),
        F.min("started_at").alias("first_started_at"),
        F.count_if(F.col("device") == "desktop").alias("desktop_events"),
    )
)
