from pyspark.sql import functions as F

result = (
    df_sessions
    .select("id", "device", F.length("country").alias("country_length"))
    .orderBy("device", "id")
)
