from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id") == 27)
    .withColumn("page_group", F.when(F.col("page").isin(["/cart", "/home"]), "Group A").otherwise("Other"))
    .withColumn("is_click", F.when(F.col("event_type") == "click", 1).otherwise(0))
)
