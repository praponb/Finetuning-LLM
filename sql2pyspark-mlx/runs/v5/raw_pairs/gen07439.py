from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.hour("event_time").between(6, 13) | F.col("duration_sec").isNotNull())
    .select(
        "id",
        "duration_sec",
        "event_time",
        F.when(F.col("duration_sec") >= 425, "High").when(F.col("duration_sec") >= 236, "Medium").otherwise("Low").alias("duration_sec_level"),
    )
    .orderBy("duration_sec", "id")
    .limit(1)
)
