from pyspark.sql import functions as F

result = (
    df_sessions
    .groupBy(F.date_trunc("QUARTER", "started_at").alias("quarter_start"))
    .agg(
        F.count_if(F.col("country") == "DE").alias("de_events"),
        F.count_if(F.col("country") == "SG").alias("sg_events"),
    )
)
