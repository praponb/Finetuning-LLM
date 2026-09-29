from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("customer_id").isNotNull() & (F.col("started_at") >= "2024-06-01"))
    .select("id", "started_at")
    .orderBy("id")
    .limit(10)
)
