from pyspark.sql import functions as F

result = (
    df_sessions
    .withColumn("activity_month", F.date_trunc("MONTH", "started_at"))
    .rollup("activity_month")
    .agg(
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("duration_min"),
        F.count(F.when(F.col("device") == "tablet", 1)).alias("tablet_count"),
        F.count(F.when(F.col("country") == "DE", 1)).alias("de_rows"),
        F.grouping("activity_month").alias("gid"),
    )
    .orderBy(F.col("duration_min").desc())
)
