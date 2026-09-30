from pyspark.sql import functions as F

result = (
    df_sessions
    .groupBy(
        F.date_trunc("week", "started_at").alias("week_start"),
        F.date_trunc("quarter", "started_at").alias("quarter_start"),
        "country",
    )
    .agg(
        F.countDistinct("device").alias("distinct_devices"),
        F.count_if(F.col("device") == "mobile").alias("mobile_events"),
        F.count("*").alias("num_rows"),
    )
)
