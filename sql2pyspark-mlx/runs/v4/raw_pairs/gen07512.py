from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(~F.col("device").isin(["tablet", "mobile"]))
    .withColumn("started_at_label", F.date_format("started_at", "yyyy-MM"))
    .orderBy("id")
)
