from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("device").isin(["tablet", "mobile"]) & ((F.col("started_at") >= "2023-11-01") | F.col("country").isin(["US", "JP", "SG"])))
    .select("id", "country", "started_at")
    .orderBy(F.col("id").desc())
    .limit(10)
)
