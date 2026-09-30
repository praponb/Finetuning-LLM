from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.hour("started_at").between(8, 13) & ((F.col("browser") == "Safari") | (F.col("device") != "mobile")))
    .select("started_at")
)
