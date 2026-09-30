from pyspark.sql import functions as F

result = (
    df_sessions
    .select("device", "country")
    .distinct()
    .orderBy("device", "country")
    .limit(5)
)
