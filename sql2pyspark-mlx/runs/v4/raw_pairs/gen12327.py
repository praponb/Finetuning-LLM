from pyspark.sql import functions as F

result = (
    df_sessions
    .groupBy(F.date_trunc("quarter", "started_at").alias("quarter_start"))
    .agg(
        F.count_if(F.col("country") == "TH").alias("th_events"),
        F.countDistinct("country").alias("distinct_countries"),
    )
)
