from pyspark.sql import functions as F

result = (
    df_sessions
    .groupBy(
        F.date_trunc("QUARTER", "started_at").alias("quarter_start"),
        F.date_trunc("WEEK", "started_at").alias("week_start"),
    )
    .agg(
        F.size(F.collect_set("device")).alias("num_devices"),
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("duration_min"),
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("span_minutes"),
        F.count_if(F.col("country") == "TH").alias("th_events"),
        F.count(F.when(F.col("device") == "mobile", 1)).alias("mobile_count"),
    )
    .filter(F.col("num_devices") > 1)
)
