from pyspark.sql import functions as F

result = (
    df_sessions
    .select("started_at")
    .orderBy("started_at")
)
