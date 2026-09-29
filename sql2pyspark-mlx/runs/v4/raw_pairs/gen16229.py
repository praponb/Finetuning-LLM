from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("browser").isin(["Firefox", "Safari", "Edge"]))
    .select("customer_id", "started_at", "device")
)
