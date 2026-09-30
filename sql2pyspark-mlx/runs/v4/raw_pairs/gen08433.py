from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.hour("started_at").between(6, 10))
    .orderBy("browser", F.col("id").desc())
)
