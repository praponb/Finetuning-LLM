from pyspark.sql import functions as F

result = (
    df_sessions
    .select("id", "device")
    .orderBy("device", F.col("id").desc())
)
