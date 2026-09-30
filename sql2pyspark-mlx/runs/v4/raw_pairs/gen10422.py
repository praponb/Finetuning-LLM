from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((F.col("device") == "desktop") & ((F.col("device") == "desktop") | F.col("customer_id").isin([4, 22])))
    .select("customer_id", "country", "browser")
)
