from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("customer_id").isin([11, 15, 33, 34]) & F.col("browser").isNotNull())
    .select("id", "country", "started_at")
    .orderBy("started_at", "id")
    .limit(1)
)
