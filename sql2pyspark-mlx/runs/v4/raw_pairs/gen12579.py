from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("device").isin(["tablet", "mobile"]) | (F.col("device") == "mobile"))
    .select("customer_id", "started_at")
)
