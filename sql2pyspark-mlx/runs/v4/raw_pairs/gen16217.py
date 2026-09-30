from pyspark.sql import functions as F

result = (
    df_sessions
    .withColumn("quarter_start", F.date_trunc("QUARTER", "started_at"))
    .groupingSets(
        [["country", "quarter_start"], ["country"], []],
        "country",
        "quarter_start",
    )
    .agg(
        F.count(F.when(F.col("country") == "SG", 1)).alias("sg_count"),
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("span_minutes"),
        F.countDistinct("browser").alias("distinct_browsers"),
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("duration_min"),
        F.grouping("country").alias("gid"),
    )
)
