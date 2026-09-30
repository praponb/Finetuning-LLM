from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.to_date("started_at") < "2023-08-01")
    .select("id", "browser", "country", "started_at")
)
