from pyspark.sql import functions as F

result = (
    df_sessions
    .select("id", "started_at", F.initcap(F.trim("country")).alias("country_title"))
    .orderBy("started_at")
)
