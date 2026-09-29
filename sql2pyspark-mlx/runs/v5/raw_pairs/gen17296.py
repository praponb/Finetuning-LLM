from pyspark.sql import functions as F

result = (
    df_sessions
    .select(
        "id",
        "started_at",
        "device",
        F.date_trunc("YEAR", "started_at").alias("started_at_period"),
    )
    .orderBy(F.col("started_at").desc())
)
