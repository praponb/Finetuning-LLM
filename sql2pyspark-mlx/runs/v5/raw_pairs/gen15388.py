from pyspark.sql import functions as F

result = (
    df_sessions
    .withColumn("quarter_start", F.date_trunc("quarter", "started_at"))
    .cube("quarter_start")
    .agg(
        F.max("started_at").alias("last_started_at"),
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("duration_min"),
        F.grouping("quarter_start").alias("gid"),
    )
    .orderBy(F.col("quarter_start").desc())
)
