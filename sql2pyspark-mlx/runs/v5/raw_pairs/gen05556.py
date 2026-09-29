from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.hour("started_at").between(8, 13))
    .select("browser")
    .intersect(df_sessions.filter(F.col("device") != "mobile").select("browser"))
)
