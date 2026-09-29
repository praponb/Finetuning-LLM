from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec").between(177, 196))
    .select("id", "duration_sec", "page")
    .orderBy(F.col("duration_sec").desc())
)
