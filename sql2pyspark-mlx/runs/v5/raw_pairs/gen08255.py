from pyspark.sql import functions as F

result = (
    df_sessions
    .withColumn("quarter_start", F.date_trunc("QUARTER", "started_at"))
    .cube("quarter_start", "browser")
    .agg(
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("span_minutes"),
        F.count(F.when(F.col("country") == "DE", 1)).alias("de_count"),
        F.countDistinct("country").alias("distinct_countries"),
        F.min("started_at").alias("first_started_at"),
        F.grouping("quarter_start").alias("grouping_level"),
    )
)
