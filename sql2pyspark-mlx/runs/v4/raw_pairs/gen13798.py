from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(~F.col("browser").isin(["Edge", "Chrome", "Firefox"]))
    .select("id", "device", "customer_id", "browser")
    .orderBy("device")
)
