from pyspark.sql import functions as F

result = (
    df_sessions
    .select("country", "device", F.hour("started_at").alias("started_at_hour"))
    .orderBy(F.col("started_at_hour").desc())
)
