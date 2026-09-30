from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("customer_id").isNull() | (F.col("browser") == "Edge"))
    .select("id", "browser", "country", "device")
    .orderBy("country", "browser")
)
