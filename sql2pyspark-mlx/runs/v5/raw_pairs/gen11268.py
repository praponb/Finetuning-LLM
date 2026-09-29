from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((F.col("country") == "SG") | (F.col("browser") == "Safari"))
    .select("country", "started_at", "browser")
)
