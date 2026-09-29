from pyspark.sql import functions as F

result = (
    df_sessions
    .select(
        "started_at",
        "device",
        F.substring("country", 1, 4).alias("country_prefix"),
    )
    .orderBy(F.col("started_at").desc())
)
