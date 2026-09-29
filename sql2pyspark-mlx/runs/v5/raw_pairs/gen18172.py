from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(((F.col("customer_id") == 40) & (F.col("device") == "mobile")) | (F.col("started_at") < "2024-12-05"))
    .select("customer_id", "country", F.hour("started_at").alias("started_at_hour"))
)
