from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("country").isin(["JP", "UK", "DE"]) & (F.col("customer_id").isNotNull() | F.col("customer_id").isNotNull()))
    .select("id", "browser", "customer_id")
    .orderBy(F.col("id").desc())
)
