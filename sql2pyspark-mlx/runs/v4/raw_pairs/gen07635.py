from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.to_date("started_at") < "2025-08-01")
    .select("id", "started_at", "country")
    .orderBy(F.col("country").desc())
)
