from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_type").isin(["purchase", "add_to_cart"]))
    .orderBy("event_type")
)
