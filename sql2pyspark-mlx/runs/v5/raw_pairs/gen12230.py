from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("customer_id").isNull() & F.col("customer_id").isin([13, 27, 39]) & F.col("browser").isin(["Edge", "Chrome"]))
    .withColumn("started_at_day", F.col("started_at").cast("date"))
    .withColumn("started_at_day_2", F.col("started_at").cast("date"))
    .orderBy("device")
)
