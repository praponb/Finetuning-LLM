from pyspark.sql import functions as F

valid = (
    df_sessions
    .filter((F.col("started_at") >= F.lit("2023-03-06 21:00:00").cast("timestamp")) & F.col("country").isNotNull())
    .select(
        "*",
        F.when(F.col("browser").isin(["Chrome", "Safari"]), "Core").otherwise("Other").alias("browser_group"),
    )
)

result = (
    valid
    .groupBy("device", F.date_trunc("YEAR", "started_at").alias("year_start"))
    .agg(
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("duration_min"),
        F.max("started_at").alias("last_seen"),
        F.min("started_at").alias("first_seen"),
        F.min("started_at").alias("first_seen_2"),
        F.count(F.when(F.col("device") == "tablet", 1)).alias("tablet_count"),
        F.count("*").alias("row_count"),
    )
    .orderBy(F.col("tablet_count").desc_nulls_last())
)
