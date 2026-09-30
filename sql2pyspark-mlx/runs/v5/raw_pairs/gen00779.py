from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("browser") == "Chrome")
    .withColumn("started_at_day", F.col("started_at").cast("date"))
    .withColumn("started_at_period", F.date_trunc("week", "started_at"))
)
