from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("country") == "UK")
    .select("customer_id", "country", F.upper("device").alias("device_upper"))
)
