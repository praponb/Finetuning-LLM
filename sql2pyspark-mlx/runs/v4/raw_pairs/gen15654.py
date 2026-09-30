from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("customer_id").isNotNull())
    .select("id", "browser")
    .subtract(df_sessions.filter(F.col("customer_id") == 16).select("id", "browser"))
    .orderBy("id")
)
