from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_type") == "add_to_cart")
    .withColumn("page_event_type", F.concat("page", F.lit("/"), "event_type"))
)
