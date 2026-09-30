from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("country") == "UK")
    .select("customer_id")
    .unionByName(df_sessions.filter(F.col("country") != "DE").select("customer_id"))
    .distinct()
    .orderBy("customer_id")
)
