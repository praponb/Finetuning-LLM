from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("device").isin(["tablet", "mobile"]))
    .select("id", "device", "customer_id", "browser")
)
