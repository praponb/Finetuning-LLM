from pyspark.sql import functions as F

result = (
    df_web_events
    .withColumn("event_type_length", F.length("event_type"))
    .withColumn("is_add_to_cart", F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0))
)
