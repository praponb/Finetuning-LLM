from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((F.col("started_at") < "2025-06-01") | ~F.col("device").isin(["mobile", "tablet"]))
    .select("id", "customer_id")
)
