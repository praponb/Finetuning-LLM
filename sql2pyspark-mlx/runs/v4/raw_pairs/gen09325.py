from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("customer_id").isNotNull())
    .select("id", "started_at", "browser", "customer_id")
    .orderBy("started_at")
)
