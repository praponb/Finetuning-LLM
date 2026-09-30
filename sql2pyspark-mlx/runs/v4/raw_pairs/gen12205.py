from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("country") == "JP")
    .select("country")
    .unionByName(df_sessions.filter(F.col("customer_id") == 39).select("country"))
)
