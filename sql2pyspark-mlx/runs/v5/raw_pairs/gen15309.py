from pyspark.sql import functions as F

result = (
    df_sessions
    .groupBy("browser")
    .agg(
        F.min("started_at").alias("first_seen"),
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("span_minutes"),
    )
    .orderBy("span_minutes")
)
