from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.hour("started_at").between(7, 14))
    .select("id", "started_at", "browser", "country")
)
