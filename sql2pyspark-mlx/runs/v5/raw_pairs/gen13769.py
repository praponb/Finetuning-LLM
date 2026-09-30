from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((F.col("country") == "US") & F.col("customer_id").isNull() & F.col("device").isin(["tablet", "desktop", "mobile"]))
    .select("started_at", "device")
)
