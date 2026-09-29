from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec") != 407)
    .withColumn("event_type_group", F.when(F.col("event_type").isin(["page_view", "purchase"]), "Group A").otherwise("Other"))
    .withColumn("duration_sec_str", F.col("duration_sec").cast("string"))
)
