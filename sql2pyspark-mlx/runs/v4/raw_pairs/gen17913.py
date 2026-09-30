from pyspark.sql import functions as F

result = (
    df_sessions
    .groupBy(F.date_trunc("MONTH", "started_at").alias("activity_month"))
    .agg(
        F.size(F.collect_set("browser")).alias("num_browsers"),
        F.count_if(F.col("browser") == "Chrome").alias("chrome_events"),
        F.count_if(F.col("device") == "desktop").alias("desktop_events"),
        F.min("started_at").alias("started_at"),
    )
)
