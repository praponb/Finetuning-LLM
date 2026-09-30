from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("device").isin(["mobile", "desktop", "tablet"]) & (F.col("started_at") < "2024-01-01") & F.col("customer_id").isNull())
    .select("id", "started_at")
)
