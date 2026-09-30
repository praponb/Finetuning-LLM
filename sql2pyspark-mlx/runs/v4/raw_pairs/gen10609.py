from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((F.col("country") == "US") | F.col("customer_id").isNotNull())
    .select("country", "started_at")
)
