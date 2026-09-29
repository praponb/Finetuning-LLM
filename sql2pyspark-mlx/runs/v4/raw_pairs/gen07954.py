from pyspark.sql import functions as F

result = (
    df_sessions
    .select("id", "started_at", "device")
    .orderBy(F.col("id").desc(), "device")
)
