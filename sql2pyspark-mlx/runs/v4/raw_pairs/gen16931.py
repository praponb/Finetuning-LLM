from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("started_at") >= "2023-07-30")
    .withColumn("browser_device", F.concat("browser", F.lit(", "), "device"))
    .withColumn("started_at_period", F.date_trunc("WEEK", "started_at"))
)
