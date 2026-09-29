from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.to_date("started_at") >= "2024-05-01")
    .select("id", "started_at", "device", "country")
)
