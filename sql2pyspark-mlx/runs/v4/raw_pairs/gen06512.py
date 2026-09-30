from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("customer_id").isin([21, 34]) | F.col("customer_id").isin([1, 15, 23]))
    .select("id", "customer_id", F.lower("device").alias("device_lower"))
    .orderBy("device_lower", F.col("customer_id").desc(), "id")
    .limit(5)
)
