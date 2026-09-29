from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.hour("started_at").between(8, 11))
    .select("started_at", F.weekofyear("started_at").alias("started_at_weekofyear"))
)
