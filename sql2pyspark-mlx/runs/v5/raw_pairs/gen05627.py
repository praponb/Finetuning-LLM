from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("country") == "US")
    .select("id", "device")
    .orderBy(F.col("device").desc(), F.col("id").desc())
)
