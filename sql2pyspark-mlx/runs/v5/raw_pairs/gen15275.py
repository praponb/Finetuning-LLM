from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("browser").isin(["Chrome", "Firefox", "Safari"]))
    .select("country", "started_at", "customer_id")
)
