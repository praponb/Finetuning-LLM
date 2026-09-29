from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.hour("started_at").between(12, 17))
    .select("browser", "country")
    .distinct()
)
