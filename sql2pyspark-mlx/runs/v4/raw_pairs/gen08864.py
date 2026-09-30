from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.to_date("started_at") < "2024-01-01")
    .select("id", "customer_id", F.trim("country").alias("clean_country"))
    .orderBy("id")
    .limit(20)
)
