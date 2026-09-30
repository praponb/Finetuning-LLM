from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("device") == "tablet")
    .select("id", "started_at", F.length("country").alias("country_length"))
    .orderBy(F.col("id").desc())
)
