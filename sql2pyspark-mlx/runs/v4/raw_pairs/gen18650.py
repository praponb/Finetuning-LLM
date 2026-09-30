from pyspark.sql import functions as F

result = (
    df_sessions
    .withColumn("started_at_label", F.date_format("started_at", "yyyy-MM"))
    .orderBy("started_at_label")
)
