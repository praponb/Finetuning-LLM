from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page", "event_type").orderBy("event_time")
w2 = Window.partitionBy("event_type").orderBy("rn", "id")
w3 = Window.partitionBy("event_type", "session_id").orderBy("event_type_length", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("session_id").orderBy(F.col("event_type_length").desc(), "id")

base = (
    df_web_events
    .filter(F.col("page").isNotNull())
    .withColumn("event_type_length", F.length("event_type"))
)

latest = (
    base
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
)

scored = (
    latest
    .select(
        "*",
        F.lag("event_type_length", 2).over(w2).alias("prev_event_type_length"),
        F.sum("event_type_length").over(w3).alias("cumulative_event_type_length"),
        F.ntile(4).over(w4).alias("event_type_length_bucket"),
    )
)

result = (
    scored
    .groupBy(
        F.when(F.col("cumulative_event_type_length") >= 407, "HIGH").otherwise("LOW").alias("cumulative_event_type_length_tier"),
        F.when(F.col("prev_event_type_length") >= 89, "HIGH").otherwise("LOW").alias("prev_event_type_length_tier"),
    )
    .agg(
        F.min("cumulative_event_type_length").alias("min_cumulative_event_type_length"),
        F.count(F.when(F.col("page") == "/cart", 1)).alias("cart_rows"),
    )
)
