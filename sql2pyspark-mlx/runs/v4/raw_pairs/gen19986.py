from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.hour("started_at").between(6, 13) & F.col("customer_id").isin([3, 21, 28, 38]))
    .select("id", "customer_id", "started_at", "device")
)
