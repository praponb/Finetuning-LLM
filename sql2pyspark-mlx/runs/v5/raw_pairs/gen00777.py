from pyspark.sql import functions as F

result = (
    df_sessions
    .rollup("device", F.date_trunc("YEAR", "started_at").alias("year_start"))
    .agg(
        F.max("started_at").alias("ended_at"),
        F.min("started_at").alias("started_at"),
        F.count(F.when(F.col("device") == "desktop", 1)).alias("desktop_count"),
    )
    .orderBy("year_start")
)
