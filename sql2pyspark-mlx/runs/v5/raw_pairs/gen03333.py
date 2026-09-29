from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("browser") == "Edge")
    .select("id", "country", F.upper("device").alias("device_upper"))
)
