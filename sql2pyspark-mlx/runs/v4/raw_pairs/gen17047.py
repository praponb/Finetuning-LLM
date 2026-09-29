from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("started_at") >= "2023-08-01")
    .select("id", "country")
)
